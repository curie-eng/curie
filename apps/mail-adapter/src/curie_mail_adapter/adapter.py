"""AgentMail ingress and email egress backed by one durable local state store."""

from __future__ import annotations

import base64
import binascii
import email.utils
import hashlib
import json
import logging
import re
import secrets
import threading
import time
import urllib.parse
from collections import OrderedDict
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Literal

from channel_protocol import SettledOutcome

from .agentmail import EGRESS_REFUSAL_ERROR, AgentMailClient, request
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
# Status 0 is a transport failure and every 4xx is a provider refusal. Neither
# can recover because the adapter repeats the same request at normal cadence, so
# both use the bounded discovery backoff. A 5xx deliberately retains its prior
# semantics: it neither arms nor clears an already armed delay.
CAUSE_MAX_CHARS = 120
AUTHENTICATION_UNVERIFIABLE = "authentication_unverifiable"

# What one channel port POST settled: admitted, refused for good by the
# binding's caller list (403, ADR 0175), or left pending for another attempt.
IngressOutcome = Literal["accepted", "refused", "retry"]

# The exact `detail` the channel port puts on a caller-list refusal. Only a 403
# carrying it is final: a 403 from a proxy or firewall in front of the platform,
# or from any other check, is an infrastructure fault and must stay retryable,
# or real mail would be dropped for good. Frozen with the platform side in
# `tests/vectors/channel-port-refusal.json`.
CALLER_NOT_ALLOWED_DETAIL = "caller_not_allowed"


def _is_caller_refusal(status: int, body: Any) -> bool:
    """Whether a channel port answer is the caller-list refusal and nothing else."""
    return (
        status == 403
        and isinstance(body, dict)
        and body.get("detail") == CALLER_NOT_ALLOWED_DETAIL
    )


# -- approvals by email (ADR-0177) ---------------------------------------------
#
# A random single-use reference links a reply to the approval it answers. It is
# not proof of identity: every reply quotes it, and anyone copied can see it.
# Current AgentMail messages cannot establish a sender authentication verdict
# Curie can verify, so the inbound gate refuses them before approval handling.
# The platform's caller and approver checks remain additional authorization.
APPROVAL_REF_PATTERN = re.compile(r"curie-approval-[A-Za-z0-9_-]{24}")
# The ReplyAck ref of a rendered card, so the worker can settle this card later.
APPROVAL_CARD_REF_PREFIX = "approval-card:"
APPROVAL_INSTRUCTIONS = (
    "To answer, reply to this email with APPROVE or REJECT on the first line. "
    "Anything after it is your note. Only an approver listed for this request can answer."
)
APPROVAL_REF_LABEL = "Approval reference:"
# The card field the worker names each of the route's listed approver addresses
# with (ADR-0177 amendment A5; ``APPROVER_FIELD_LABEL`` in
# ``curie_worker.approvals``). Read only to word the emails: who may answer is
# still the platform's decision.
APPROVER_FIELD_LABEL = "Approver"
NOT_AN_APPROVER = "You are not an approver for this request."
APPROVAL_NOTE_MAX_CHARS = 4000
DECISIONS = {"APPROVE": "approved", "REJECT": "rejected"}
APPROVAL_ACTOR_HEADER = "X-Curie-Approval-Actor"
ADAPTER_PRINCIPAL_HEADER = "X-Curie-Adapter-Principal"
# RFC 3834 section 5 (Auto-Submitted), plus the de facto markers vacation
# responders and list managers set. Header names compare case-insensitively.
_AUTO_PRECEDENCE = frozenset({"bulk", "junk", "list", "auto_reply"})
_AUTO_REPLY_HEADERS = ("x-autoreply", "x-autorespond", "x-auto-response")
_BOUNCE_LOCAL_PARTS = frozenset({"mailer-daemon", "postmaster"})

AnswerOutcome = Literal["resolved", "not_an_answer", "retry"]


def _poll_should_back_off(status: int) -> bool:
    return status == 0 or 400 <= status < 500


class ProviderThreadDeletedError(RuntimeError):
    """The provider definitively rejected the thread lookup with HTTP 404."""


class ProviderEgressRefusedError(RuntimeError):
    """The outbound AgentMail connection was refused before an HTTP response."""


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
        self.last_ingress_status: int | None = None
        self.channel_token_rejected = False
        self.seen: OrderedDict[str, bool] = OrderedDict()
        for message_id in self.state.known_message_ids():
            self._mark_seen(message_id)
        # Opaque pagination is a discovery hint only. Durable pending ids, never
        # this cursor, are the source of truth across a restart.
        self.page_cursor: str | None = None
        # Discovery health (#2731): the monotonic start of the current failure
        # run and its length. A run older than the configured threshold makes
        # /readyz report 503 while /healthz stays 200. State is judged as of the
        # latest recorded pass, so it only moves when the poll loop observes.
        self.discovery_failing_since: float | None = None
        self.discovery_failures = 0
        self.discovery_last_failure_at = 0.0
        self.discovery_unreachable_logged = False

    def status(self) -> dict[str, Any]:
        """Report diagnostic claims only; the adapter cannot verify the signature."""
        token = self.config.channel_token
        exp = _token_expiry(token)
        with self.lock:
            rejected = self.channel_token_rejected
            last_status = self.last_ingress_status
            discovery = self._discovery_snapshot()
        now = time.time()
        state = "ok"
        if not self.config.ingress_enabled:
            state = "disabled"
        elif not token:
            state = "missing"
        elif exp is None:
            state = "invalid"
        elif exp <= now:
            state = "expired"
        elif rejected:
            state = "rejected"
        elif exp <= now + 300:
            state = "expiring"
        return {
            "status": "ready" if self.ready.is_set() else "starting",
            "channel_token": {"present": bool(token), "exp": exp, "state": state},
            "last_ingress_status": last_status,
            "discovery": discovery,
        }

    def _discovery_snapshot(self) -> dict[str, Any]:
        since = self.discovery_failing_since
        if since is None:
            return {"state": "ok", "consecutive_failures": 0, "failing_for_seconds": 0.0}
        failing_for = max(0.0, self.discovery_last_failure_at - since)
        unreachable = failing_for >= self.config.discovery_unready_after_seconds
        return {
            "state": "unreachable" if unreachable else "failing",
            "consecutive_failures": self.discovery_failures,
            "failing_for_seconds": failing_for,
        }

    def record_discovery(self, status: int, now: float | None = None) -> None:
        """Fold one discovery pass status into readiness; only 200 clears a failure run."""
        at = time.monotonic() if now is None else now
        with self.lock:
            if status == 200:
                recovered = self.discovery_unreachable_logged
                failures = self.discovery_failures
                self.discovery_failing_since = None
                self.discovery_failures = 0
                self.discovery_unreachable_logged = False
                if recovered:
                    logger.info("discovery recovered after %s consecutive failures", failures)
                return
            if self.discovery_failing_since is None:
                self.discovery_failing_since = at
            self.discovery_failures += 1
            self.discovery_last_failure_at = at
            snapshot = self._discovery_snapshot()
            if snapshot["state"] == "unreachable" and not self.discovery_unreachable_logged:
                self.discovery_unreachable_logged = True
                logger.error(
                    "discovery unreachable: %s consecutive failures over %.1fs, last status=%s; "
                    "readiness now reports 503",
                    snapshot["consecutive_failures"],
                    snapshot["failing_for_seconds"],
                    status,
                )

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
                    admission = self.state.record_terminal(message_id, "rejected")
                    if admission == "full":
                        succeeded = False
                        logger.error("prime: durable state capacity reached before completion")
                        break
                    self._mark_seen(message_id)
                    if admission == "admitted":
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
            self.record_discovery(status)
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
            self.record_discovery(status)
            if _poll_should_back_off(status):
                backoff = min(backoff * 2 + BACKOFF_STEP_SECONDS, BACKOFF_MAX_SECONDS)
                logger.warning(
                    "poll: status=%s, backing off %ss before the next discovery pass",
                    status,
                    backoff,
                )
            # Only a successful listing proves the provider recovered, so it is the
            # only thing that clears the delay. A 5xx does not arm the backoff -
            # that stays deliberately out of this issue's scope - but it must not be
            # able to un-arm one either: an outage is rarely one failure mode end to
            # end, and letting a mid-outage 500 reset the delay puts the poller back
            # on its normal cadence in the middle of the outage, which is the exact
            # warning burst this backoff exists to stop.
            elif status == 200:
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
                logger.warning(
                    "poll: list failed with status=%s cause=%s",
                    status,
                    self._transport_cause(status, page),
                )
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
        return status

    def _transport_cause(self, status: int, body: Any) -> str:
        """Render a failed list call's cause as a bounded, credential-free string.

        Status is the gate, not the body's shape. Only a status 0 body is one
        this package synthesized locally: ``agentmail.request`` builds
        ``{"error": "connection_refused"}`` for a refused TCP connection,
        ``{"error": str(exc)}`` for another ``OSError``, and ``{"error": "response
        body exceeds configured byte limit"}`` for an oversize response, all
        local strings with a shape the adapter can reason about. Every other
        status carries a body the PROVIDER authored - arbitrary, unbounded, and
        able to carry mail content, an upstream stack trace or a page of HTML -
        including one that happens to hold an ``error`` key. Such a body is
        never rendered, only counted by its status code, because logging any
        part of it would breach this package's no-mail-PII rule and push
        whatever the provider felt like sending into the cluster's log
        retention. That is a structural refusal at the top of this helper rather
        than a condition at the call site, so a future caller cannot reintroduce
        the exposure by passing a provider payload in.

        The budget is a fixed constant rather than a config knob because the
        size of a log line must not be something a hostile or broken provider
        can grow, and an operator cannot tune a value they only discover after
        the disk filled. The redaction pass is defence in depth: ``str(OSError)``
        from urllib carries no request header today, but the adapter must not
        depend on a stdlib rendering staying that way.
        """
        if status != 0:
            # The status line is the only provider-authored response metadata
            # admitted into this diagnostic. Valid HTTP codes have a fixed
            # three-digit budget; an impossible value gets one fixed fallback
            # rather than an unbounded rendering of the integer.
            return f"http_{status}" if 100 <= status <= 599 else "http_invalid"
        if not isinstance(body, dict):
            return "unavailable"
        error = body.get("error")
        if not isinstance(error, str) or not error.strip():
            return "unavailable"
        cause = " ".join(error.split())
        for credential in (
            self.config.agentmail_api_key,
            self.config.channel_token,
            self.config.egress_secret,
        ):
            if credential:
                cause = cause.replace(credential, "[redacted]")
        if len(cause) > CAUSE_MAX_CHARS:
            cause = cause[:CAUSE_MAX_CHARS] + "..."
        return cause

    def _retry_pending(self) -> None:
        for pending in self.state.pending():
            try:
                rejection = self._listing_rejection(pending["summary"])
                self._refuse_inbound(pending["message_id"], rejection)
            except Exception:  # noqa: BLE001 - existing broad catch retained
                logger.error(
                    "poll: retrying correlation=%s failed unexpectedly",
                    _correlation(pending["message_id"]),
                )

    def handle_inbound(self, message: dict[str, Any]) -> bool:
        """Refuse current AgentMail intake before body fetch or approval handling."""
        message_id = str(message["message_id"])
        rejection = self._listing_rejection(message)
        return self._refuse_inbound(message_id, rejection)

    def _listing_rejection(self, message: dict[str, Any]) -> tuple[str, str]:
        """Refuse metadata that cannot prove positive sender authentication.

        AgentMail exposes labels and arbitrary headers, without a trusted
        aligned verdict or header provenance guarantee. Neither those fields
        nor an allowed sender can supply a verdict Curie verifies itself.
        Until such evidence exists, every AgentMail message is refused.
        https://docs.agentmail.to/api-reference/inboxes/messages/get
        https://docs.agentmail.to/knowledge-base/inbound-emails-missing
        """
        return (AUTHENTICATION_UNVERIFIABLE, "")

    def _log_listing_rejection(
        self, message_id: str, rejection: tuple[str, str]
    ) -> None:
        reason, _ = rejection
        logger.warning(
            "rejected correlation=%s: reason=%s",
            _correlation(message_id),
            reason,
        )

    def _refuse_inbound(self, message_id: str, rejection: tuple[str, str]) -> bool:
        """Settle unstarted intake while preserving work already admitted."""
        known = self.state.delivery(message_id)
        if known is None:
            if self.state.record_terminal(message_id, "rejected") == "full":
                return False
        elif known["state"] != "accepted":
            turn = known["turn"]
            if turn is not None:
                self.state.finish_reply(str(turn["conversation_id"]), str(turn["reply_ref"]))
            self.state.settle_without_turn(message_id, "rejected")
        # A historical accepted delivery may still owe egress. Its stored reply
        # ownership survives, but it must never post another unauthenticated turn.
        self._log_listing_rejection(message_id, rejection)
        return True

    def sender_allowed(self, from_header: str) -> bool:
        address = _bare_address(from_header)
        domain = address.rpartition("@")[2]
        for entry in self.config.allowed_senders:
            candidate = entry.strip().lower()
            if candidate == "*" and self.config.allow_all_senders:
                return True
            if candidate == address:
                return True
            if "@" not in candidate and candidate and candidate == domain:
                return True
        return False

    def post_turn(self, turn: dict[str, Any]) -> IngressOutcome:
        """Post one turn to the channel port and classify the platform's answer.

        Returns:
            ``"accepted"`` only for the platform's terminal 200 admission;
            ``"refused"`` for a 403 whose ``detail`` is ``caller_not_allowed``,
            which the channel port answers when the binding's caller list does
            not admit the sender (ADR 0175) and which is final for every
            adapter; ``"retry"`` for everything else, any other 403 included,
            which leaves the delivery pending under the same stable id.
        """
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
            with self.lock:
                self.last_ingress_status = result.status
                was_rejected = self.channel_token_rejected
                if result.status == 401:
                    self.channel_token_rejected = True
                elif result.status == 200:
                    self.channel_token_rejected = False
            if result.status == 401 and not was_rejected:
                exp = _token_expiry(self.config.channel_token)
                expires = datetime.fromtimestamp(exp, UTC).isoformat() if exp else "unknown"
                logger.warning(
                    "channel token rejected by the platform (exp %s); re-mint with "
                    "POST /channels/token using the platform X-API-Key and install the "
                    "replacement CURIE_CHANNEL_TOKEN, then restart the adapter",
                    expires,
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
                return "accepted"
            if _is_caller_refusal(result.status, result.body):
                logger.warning(
                    "ingress refused correlation=%s: the binding's caller list does not "
                    "admit this sender; settling the message without a turn",
                    _correlation(str(turn["delivery_id"])),
                )
                return "refused"
            if result.status == 429:
                retry_after = _retry_after_seconds(result.headers)
                if retry_after > 0:
                    self.shutdown.wait(retry_after)
            return "retry"
        logger.warning(
            "ingress unreachable; correlation=%s remains pending",
            _correlation(str(turn["delivery_id"])),
        )
        return "retry"

    # -- approvals by email (ADR-0177) ---------------------------------------

    def _handle_approval_reply(
        self,
        message_id: str,
        conversation_id: str,
        sender: str,
        full: dict[str, Any],
    ) -> bool | None:
        """Handle a message in a thread this adapter rendered an approval in.

        Returns None when the message is an ordinary turn: the thread has no
        live approval and the message names no reference. Otherwise the message
        is never a turn (ADR-0106): it is an answer, or it gets the instructions
        back, and the return value is ``handle_inbound``'s.

        Current AgentMail ingress has no path to this interpreter:
        ``handle_inbound`` refuses every message before body fetch because
        Curie cannot verify a positive sender authentication verdict. Sender
        filtering and this reference interpreter cannot establish that verdict.
        Its remaining checks require a reference issued in this thread, still
        live, the message was not sent automatically, and the first line of its
        new text is one decision word. Who may answer is not decided here: the
        verified sender is carried to the platform, which checks the binding's
        ``allowed_callers`` and the route's approver emails.
        """
        refs = self.state.approval_refs_in(conversation_id)
        if not refs:
            return None
        named = set(
            APPROVAL_REF_PATTERN.findall(
                " ".join(str(full.get(field) or "") for field in ("extracted_text", "text"))
            )
        )
        matched = [ref for ref in refs if ref["reference"] in named]
        live = [ref for ref in refs if ref["state"] == "live"]
        if not matched and not live:
            # Every approval in this thread is over: the conversation goes on.
            return None
        correlation = _correlation(message_id)
        if _sent_automatically(full, sender):
            # Never answer an auto-reply, an out-of-office or a bounce: a
            # response invites a mail loop, and its words are nobody's decision.
            logger.info("approval reply correlation=%s ignored: sent automatically", correlation)
            self.state.settle_without_turn(message_id, "answered")
            return True
        ref = matched[-1] if matched else live[-1]
        decision, note = _parse_decision(full) if matched else (None, None)
        copies_in = decision is None and _brings_in_an_approver(ref, sender, full)
        if ref["state"] == "live" and copies_in:
            # ADR-0177 amendment A5: the requester did what the request email
            # asked, replying all with a listed approver copied in. That
            # approver now has the request; answering the requester back with
            # the instructions would only suggest they got it wrong.
            logger.info("approval reply correlation=%s copied in an approver", correlation)
        elif not matched:
            self._notify(message_id, APPROVAL_INSTRUCTIONS, correlation)
        elif ref["state"] != "live":
            self._notify(message_id, "This approval has already been answered.", correlation)
        elif decision is None:
            self._notify(message_id, APPROVAL_INSTRUCTIONS, correlation)
        else:
            # The bare address the inbound gate verified, lowercased, never the
            # display name from the From header: that is the only part of it
            # anyone vouched for, and the form the platform's lists are in.
            outcome = self._carry_answer(
                ref, _bare_address(sender), decision, note, message_id, full
            )
            if outcome == "retry":
                # The platform could not be asked. Keep the message pending so
                # the next pass carries the same answer again; resolve is
                # resolve-once, so a repeat cannot decide twice.
                return self.state.body_failed(message_id, abandon_after=BODY_ATTEMPT_MAX)
        self.state.settle_without_turn(message_id, "answered")
        return True

    def _carry_answer(
        self,
        ref: dict[str, Any],
        actor: str,
        decision: str,
        note: str | None,
        message_id: str,
        full: dict[str, Any],
    ) -> AnswerOutcome:
        """Resolve the approval with the adapter's credential and the sender as actor.

        The platform decides. A win reopens the asking message's reply owner so
        the resumed turn can answer on it, and remembers this message and who is
        on it: the follow-up, sent when the card is settled whatever ended the
        approval, goes to everyone on the winning answer (ADR-0177 amendment A5).
        """
        url = (
            f"{self.config.api_base_url.rstrip('/')}/approvals/"
            f"{urllib.parse.quote(ref['approval_id'], safe='')}/resolve"
        )
        body: dict[str, Any] = {"decision": decision}
        if note:
            body["note"] = note
        result = request(
            "POST",
            url,
            body,
            {ADAPTER_PRINCIPAL_HEADER: self.config.adapter_principal, APPROVAL_ACTOR_HEADER: actor},
            max_response_bytes=self.config.max_body_bytes,
        )
        correlation = _correlation(message_id)
        logger.info("approval answer correlation=%s status=%s", correlation, result.status)
        if result.status == 200:
            self.state.record_approval_answer(
                ref["reference"], message_id, sorted(self._participants(full))
            )
            self.state.set_approval_ref_state(ref["reference"], "answered")
            self.state.reopen_reply(ref["conversation_id"], ref["reply_ref"])
            return "resolved"
        if result.status == 0 or result.status == 401 or result.status >= 500:
            # 401 is this adapter's own credential lapsing: an operator re-mints
            # it, and the pending answer then goes through, like CURIE_CHANNEL_TOKEN.
            return "retry"
        if result.status in (409, 410):
            # Over, but not spent: the card's settlement still owes the one
            # follow-up and must reopen the asking reply for the resumed turn.
            # A lost 200 retried into a 409 lands here too, so a 409 records
            # this message as the answer when none is recorded yet.
            if result.status == 409:
                self.state.record_approval_answer(
                    ref["reference"], message_id, sorted(self._participants(full))
                )
            self.state.set_approval_ref_state(ref["reference"], "answered")
            text = (
                "This approval has already been answered."
                if result.status == 409
                else "This approval expired before it was answered."
            )
        elif _is_caller_refusal(result.status, result.body):
            # The binding's allowed_callers do not admit this sender (ADR 0175
            # decision 3): a refused caller gets nothing back, on an answer
            # exactly as on a turn.
            logger.info("approval answer correlation=%s refused: caller not allowed", correlation)
            return "not_an_answer"
        elif result.status == 403:
            text = NOT_AN_APPROVER
            if ref["approvers"]:
                text += " " + _who_can_approve(ref["approvers"])
        else:
            text = "Your answer could not be accepted for this approval."
        self._notify(message_id, text, correlation)
        return "not_an_answer"

    def _notify(self, message_id: str, text: str, correlation: str) -> None:
        """Send one short reply to ``message_id``, best effort.

        These are notices about an answer, not an agent's reply: a lost one is
        a smaller wrong than a duplicate, so there is no durable retry.
        """
        status, _body = self.client.reply(message_id, text)
        if not 200 <= status < 300:
            logger.warning(
                "approval notice correlation=%s not sent: status=%s", correlation, status
            )

    def record_approval_card(
        self,
        conversation_id: str,
        approval_id: str,
        text: str,
        *,
        requester: str = "",
        approvers: Sequence[str] = (),
    ) -> tuple[int, str | None]:
        """Render an approval card into the pending reply, with a fresh reference.

        The request says who can approve (ADR-0177 amendment A5): the route's
        listed addresses, and whether any of them is on the thread already (the
        requester, or the To or Cc of the asking message). When none is, it asks
        the requester to reply all and copy one or more of them in. The request
        is mailed reply all, so a listed approver copied on the asking message
        receives it. Nobody off the thread is mailed.

        Args:
            conversation_id: the thread the card belongs to.
            approval_id: the platform's approval id.
            text: the card's text.
            requester: the ``requested_by`` of the card, the asking sender.
            approvers: the card's ``Approver`` fields; empty from a worker that
                sends none, which keeps the generic instructions.

        Returns:
            The ack status and the card ref the worker keeps to settle this
            card. Without an adapter principal the card is recorded as plain
            text, exactly as before, and no ref is returned: nothing here could
            carry an answer, so nothing invites one.
        """
        if not self.config.adapter_principal:
            return self.record_text(conversation_id, None, text, append=True), None
        refs = self.state.live_reply_refs(conversation_id)
        if len(refs) != 1:
            logger.info(
                "approval card deferred: correlation=%s has %d live reply refs",
                _correlation(conversation_id),
                len(refs),
            )
            return 503, None
        reply_ref = refs[0]
        listed = _listed_addresses(approvers)
        requester = _bare_address(requester)
        reference = self.state.issue_approval_ref(
            approval_id,
            conversation_id,
            reply_ref,
            f"curie-approval-{secrets.token_urlsafe(18)}",
            requester=requester,
            approvers=listed,
        )
        instructions = APPROVAL_INSTRUCTIONS
        if listed:
            on_thread = self._on_thread(reply_ref, requester, listed)
            instructions = _request_instructions(listed, on_thread)
        card = f"{text}\n\n{instructions}\n{APPROVAL_REF_LABEL} {reference}"
        status = self.record_text(conversation_id, reply_ref, card, append=True)
        if status != 200:
            return status, None
        return 200, f"{APPROVAL_CARD_REF_PREFIX}{approval_id}"

    def settle_approval_card(self, card_ref: str, settled: SettledOutcome) -> int:
        """Send the one follow-up for a settled card and spend its reference.

        A sent email cannot be edited, so settling is a short reply in the
        thread (ADR-0177 decision 6). The asking message's reply owner is
        reopened first, so the resumed turn's answer can follow it.
        """
        approval_id = card_ref[len(APPROVAL_CARD_REF_PREFIX) :]
        ref = self.state.approval_ref_for(approval_id)
        if ref is None or not self.state.claim_approval_settlement(ref["reference"]):
            # Unknown, already spent, or another delivery holds the send.
            return 200
        self.state.reopen_reply(ref["conversation_id"], ref["reply_ref"])
        # Read again under the claim: a send counted by an earlier settlement
        # that failed part way is not made twice.
        ref = self.state.approval_ref_for(approval_id) or ref
        if settled.decision is None:
            text = "This approval expired before anyone answered it."
        else:
            text = f"This request was {settled.decision}"
            if settled.resolver:
                text += f" by {settled.resolver}"
            text += "."
            if settled.note:
                text += f"\n\nNote: {settled.note}"
        sends = _follow_up_sends(ref)
        for index, (message_id, reply_all) in enumerate(sends):
            if index < ref["follow_ups_sent"]:
                continue
            status, _body = self.client.reply(message_id, text, reply_all=reply_all)
            if not 200 <= status < 300:
                logger.warning(
                    "approval follow-up correlation=%s failed: status=%s",
                    _correlation(approval_id),
                    status,
                )
                self.state.release_approval_settlement(ref["reference"])
                return 502
            self.state.record_follow_ups_sent(ref["reference"], index + 1)
        self.state.set_approval_ref_state(ref["reference"], "spent")
        return 200

    def _participants(self, full: dict[str, Any]) -> set[str]:
        """The bare addresses on one message, From, To and Cc, without this inbox."""
        found: set[str] = set()
        for field in ("from", "to", "cc"):
            value = full.get(field)
            for entry in value if isinstance(value, list) else [value]:
                address = _bare_address(str(entry or ""))
                if address:
                    found.add(address)
        found.discard(self.config.agentmail_inbox.strip().lower())
        return found

    def _on_thread(self, reply_ref: str, requester: str, listed: list[str]) -> list[str] | None:
        """The listed approvers already on the asking message, or None if unknown.

        The requester is on the thread by definition. The rest comes from the
        asking message's To and Cc, read from the provider; when it cannot be
        read and the requester is not listed, the answer is unknown, and the
        request is worded so it holds either way.
        """
        status, full = self.client.get_message(reply_ref)
        if status == 200 and isinstance(full, dict):
            present = self._participants(full) | {requester}
            return [address for address in listed if address in present]
        if requester in listed:
            return [requester]
        return None

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
        return self.record_text_at(conversation_id, reply_ref, text, append=append)[0]

    def record_text_at(
        self,
        conversation_id: str,
        reply_ref: str | None,
        text: str | None,
        *,
        append: bool = False,
    ) -> tuple[int, str | None]:
        """``record_text``, also naming the ref the text was recorded at.

        Text with no ``reply_ref`` lands on the conversation's one live reply
        owner. Naming it lets the ack hand that ref back, so the worker keeps
        the rest of the turn, and its completion, on the same message. That is
        how a resumed approval turn, which drops the replayed placeholder to
        answer after the card (ADR-0179 decision 3), is still mailed as a reply
        to the asking message. None when nothing was recorded.
        """
        if not conversation_id or not text:
            return 200, None
        chosen_ref = reply_ref
        if not chosen_ref:
            refs = self.state.live_reply_refs(conversation_id)
            if len(refs) != 1:
                logger.info(
                    "reply post deferred: correlation=%s has %d live reply refs",
                    _correlation(conversation_id),
                    len(refs),
                )
                return 503, None
            chosen_ref = refs[0]
        outcome = self.state.record_text(
            conversation_id,
            chosen_ref,
            text,
            append=append,
            max_bytes=self.config.max_reply_bytes,
        )
        if outcome == "too_large":
            return 413, None
        if outcome == "missing":
            logger.info(
                "reply update deferred: no active admitted owner for correlation=%s",
                _correlation(f"{conversation_id}\0{chosen_ref}"),
            )
            return 503, None
        return 200, chosen_ref

    def thread_carries(self, conversation_id: str, event_id: str) -> bool | None:
        status, thread = self.client.get_thread(conversation_id)
        if (
            status == 0
            and isinstance(thread, dict)
            and thread.get("error") == EGRESS_REFUSAL_ERROR
        ):
            raise ProviderEgressRefusedError
        if status == 404:
            # Only the PROVIDER's own answer is deletion. `request` parses a JSON
            # body and hands back the raw text when it is not JSON, so a 404
            # carrying anything but an object came from something between us and
            # AgentMail -- an edge or gateway 404 page, a stale route mid-deploy,
            # a path that never reached the API. Believing one of those writes a
            # PERMANENT tombstone (`delete_event`) after which no retry ever
            # calls the provider again, and the reply is lost with the release
            # otherwise healthy. Ambiguity retries; only a confirmed answer is
            # terminal, which is the invariant CLAUDE.md already states.
            if not isinstance(thread, dict):
                logger.warning(
                    "thread listing -> 404 with no provider body; durable witness unreadable"
                )
                return None
            raise ProviderThreadDeletedError
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

    def send_reply(
        self,
        event_id: str,
        conversation_id: str,
        reply_ref: str | None,
        *,
        outcome: str = "delivered",
    ) -> int:
        """Apply the provider witness recovery decision."""
        if not reply_ref:
            logger.info(
                "reply skipped: correlation=%s carries no reply_ref",
                _correlation(event_id),
            )
            return 200
        claim = self.state.claim_event(event_id, conversation_id, reply_ref, self.owner)
        if claim == "deleted":
            return 410
        if claim == "done":
            return 200
        if claim == "busy":
            return 503
        try:
            if outcome == "dropped":
                exists, text = self.state.reply_text(conversation_id, reply_ref)
                if not text:
                    # A turn dropped before it said anything owes its sender no
                    # mail; the empty-reply notice would be a new message, and
                    # from a sibling inbox the next turn of the exchange the
                    # drop ended (ADR-0168 decision 6).
                    self.state.finish_event(event_id)
                    if exists:
                        self.state.finish_reply(conversation_id, reply_ref)
                    logger.info(
                        "reply skipped: dropped correlation=%s recorded no text",
                        _correlation(event_id),
                    )
                    return 200
            try:
                carries = self.thread_carries(conversation_id, event_id)
            except ProviderEgressRefusedError:
                logger.warning(
                    "provider egress connection refused during thread witness; correlation=%s",
                    _correlation(event_id),
                )
                return 424
            except ProviderThreadDeletedError:
                exists, _text = self.state.reply_text(conversation_id, reply_ref)
                if not exists:
                    return 502
                self.state.delete_event(event_id, conversation_id, reply_ref)
                logger.warning(
                    "reply terminal: thread deleted at provider; correlation=%s",
                    _correlation(event_id),
                )
                return 410
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
            # The request email goes to everyone on the asking message, so a
            # listed approver copied there receives it (ADR-0177 amendment A5).
            # Every other reply, the resumed answer included, goes to the
            # sender, which on that message is the requester.
            reply_all = self.state.live_approval_on(conversation_id, reply_ref)
            status, response = self.client.reply(reply_ref, body, reply_all=reply_all)
            if (
                status == 0
                and isinstance(response, dict)
                and response.get("error") == EGRESS_REFUSAL_ERROR
            ):
                logger.warning(
                    "provider egress connection refused during send; correlation=%s",
                    _correlation(event_id),
                )
                return 424
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


def _listed_addresses(approvers: Iterable[str]) -> list[str]:
    """The card's approver addresses: bare, lowercased, once each, in order."""
    seen: dict[str, None] = {}
    for entry in approvers:
        address = _bare_address(entry)
        if "@" in address:
            seen.setdefault(address, None)
    return list(seen)


def _who_can_approve(listed: list[str]) -> str:
    """One sentence naming who can approve and how to bring them in."""
    if len(listed) == 1:
        return f"Only {listed[0]} can approve it. Reply all to this email and add {listed[0]}."
    return (
        f"Only these addresses can approve it: {', '.join(listed)}. Reply all to this email "
        "and add one or more of them, as many as you like."
    )


def _request_instructions(listed: list[str], on_thread: list[str] | None) -> str:
    """How to answer, worded for who is on the thread (ADR-0177 amendment A5).

    Args:
        listed: the route's listed approver addresses, never empty.
        on_thread: the listed addresses already on the thread, or None when
            the asking message could not be read.

    Returns:
        The instructions placed above the reference in the request email.
    """
    answer = (
        "with APPROVE or REJECT on the first line. Anything after it is the note. "
        "The first answer decides, and it is final."
    )
    if on_thread:
        return (
            f"Who can approve: {', '.join(listed)}.\n"
            f"Already on this thread and able to answer: {', '.join(on_thread)}.\n"
            f"To answer, reply all to this email {answer}"
        )
    if on_thread is None:
        lead = "If none of the people who can approve is on this thread yet:"
    else:
        lead = "Nobody on this thread can approve this request yet."
    return (
        f"{lead} {_who_can_approve(listed)}\n"
        f"Anyone listed who is on the thread can then answer by replying all {answer}"
    )


def _brings_in_an_approver(ref: dict[str, Any], sender: str, full: dict[str, Any]) -> bool:
    """Whether a non-answer from someone not listed copies a listed approver in."""
    listed = set(ref.get("approvers") or [])
    if not listed or _bare_address(sender) in listed:
        return False
    copied = set()
    for field in ("to", "cc"):
        value = full.get(field)
        for entry in value if isinstance(value, list) else [value]:
            copied.add(_bare_address(str(entry or "")))
    return bool(listed & copied)


def _follow_up_sends(ref: dict[str, Any]) -> list[tuple[str, bool]]:
    """Where a settled card's outcome goes: (message to reply to, reply all).

    Reply all to the message that carried the winning answer, so the approver
    and everyone on it see who decided. When no email answered it (expiry, or
    an answer this adapter did not carry), reply all to the asking message. If
    the requester is not on the winning message, because the approver replied
    to the bot alone, they also get the outcome as a direct reply to the asking
    message, whose sender they are (ADR-0177 amendment A5).
    """
    asking = str(ref["reply_ref"])
    answer = ref.get("answer_message_id")
    if not answer or answer == asking:
        return [(asking, True)]
    sends = [(str(answer), True)]
    requester = ref.get("requester") or ""
    if not requester or requester not in set(ref.get("answer_participants") or []):
        sends.append((asking, False))
    return sends


def _sent_automatically(full: dict[str, Any], sender: str) -> bool:
    """Whether a message was sent by software rather than a person.

    Fails closed: a message whose headers the provider did not return cannot
    be shown to be a person's, so it is treated as automatic. AgentMail
    documents ``headers`` as an optional map from string to string
    (https://docs.agentmail.to/api-reference/inboxes/messages/get).
    """
    headers = full.get("headers")
    if not isinstance(headers, dict):
        return True
    lowered = {str(key).lower(): str(value).strip().lower() for key, value in headers.items()}
    auto_submitted = lowered.get("auto-submitted")
    if auto_submitted is not None and auto_submitted != "no":
        return True
    if any(name in lowered for name in _AUTO_REPLY_HEADERS):
        return True
    if lowered.get("precedence") in _AUTO_PRECEDENCE:
        return True
    if lowered.get("content-type", "").startswith("multipart/report"):
        return True
    if lowered.get("return-path") == "<>":
        return True
    return _bare_address(sender).partition("@")[0] in _BOUNCE_LOCAL_PARTS


def _parse_decision(full: dict[str, Any]) -> tuple[str | None, str | None]:
    """The decision on the first line of the NEW text, and the rest as the note.

    Only ``extracted_text``: AgentMail strips "quoted history and trailing
    boilerplate" from it (https://www.agentmail.to/docs/messages), so a decision
    word that appears only inside the quoted request is never read as an
    answer. A message without it has no new text this can trust.
    """
    new_text = full.get("extracted_text")
    if not isinstance(new_text, str):
        return None, None
    lines = new_text.strip().splitlines()
    if not lines:
        return None, None
    word = lines[0].strip().rstrip(".!").strip().upper()
    decision = DECISIONS.get(word)
    if decision is None:
        return None, None
    note = "\n".join(lines[1:]).strip()[:APPROVAL_NOTE_MAX_CHARS]
    return decision, note or None


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


def _token_expiry(token: str) -> int | None:
    """Read an unverified exp from the platform's chn.payload.signature format.

    This diagnoses expiry without giving the adapter a platform signing key.
    Only the platform ingress response can establish credential acceptance.
    """
    if len(token) > 16_384:
        return None
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != "chn" or not parts[2]:
        return None
    try:
        payload = json.loads(
            base64.b64decode(
                parts[1] + "=" * (-len(parts[1]) % 4),
                altchars=b"-_",
                validate=True,
            )
        )
    except (ValueError, UnicodeDecodeError, binascii.Error):
        return None
    exp = payload.get("exp") if isinstance(payload, dict) else None
    # bool is an int subclass, and timestamps outside datetime's range cannot
    # be safely formatted in the operator diagnosis.
    if isinstance(exp, int) and not isinstance(exp, bool) and 0 < exp < 253_402_300_800:
        return exp
    return None
