"""A conformance kit for mail providers (ADR 0228 decision 5).

Plain data and plain functions with no test runner in it, shaped like
``channel_protocol.conformance``, so a provider author outside this repository
runs it from their own tests against a fake or a sandbox mailbox of their mail
service. The author wraps their provider in an object satisfying
``MailProviderSubject``: it holds the provider and can place a message in the
inbox, report who a send reached, arrange the next call to fail, and delete a
thread. Then run every check:

    for check in CHECKS:
        check.run(subject)

A check that finds a violation raises ``ConformanceFailure`` naming the check,
what it expected and what it observed. Passing the kit is necessary, not
sufficient: the provider sits inside the approval trust boundary, and check 4
against an author's own fake mostly proves the fake. Installing a provider is a
trust decision equal to handing it the adapter principal.
"""

from __future__ import annotations

import email.utils
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from .provider import (
    FAILURE_CAUSE_MAX_CHARS,
    FAILURE_KINDS,
    Failure,
    FailureKind,
    MailProvider,
    Message,
)

__all__ = [
    "CHECKS",
    "Check",
    "ConformanceFailure",
    "MailProviderSubject",
]

# The operations a subject can be asked to make fail on their next call.
Operation = str  # "list", "get", "send" or "thread"


class MailProviderSubject(Protocol):
    """A provider under test and the hooks the kit needs around it."""

    provider: MailProvider

    def place(
        self,
        *,
        sender: str,
        subject: str,
        new_text: str | None,
        full_text: str | None = None,
        html: str | None = None,
        reply_to: str | None = None,
        extra_senders: tuple[str, ...] = (),
        cc: tuple[str, ...] = (),
        authenticated: bool = False,
        forged_authentication: bool = False,
        sent_by_inbox: bool = False,
    ) -> str:
        """Put one message in the inbox and return its message id.

        ``reply_to`` names a message this one replies to in its thread.
        ``authenticated`` asks the receiving mail system to authenticate the
        sender; ``forged_authentication`` adds sender-written authentication
        headers that claim a pass. ``sent_by_inbox`` makes it a message this
        inbox sent.
        """
        ...

    def recipients_of_last_send(self) -> frozenset[str]:
        """The bare, lowercased addresses the last accepted send reached."""
        ...

    def fail_next(self, operation: Operation, kind: FailureKind) -> bool:
        """Make the next ``operation`` call fail with ``kind``; False if impossible."""
        ...

    def delete_thread(self, thread_id: str) -> None:
        """Delete a thread at the service, so its next lookup is definitive."""
        ...

    def credentials(self) -> tuple[str, ...]:
        """Every credential the provider holds, which no failure cause may contain."""
        ...


class ConformanceFailure(AssertionError):
    """A provider did not do what the interface promises."""


@dataclass(frozen=True)
class Check:
    """One conformance check: its number and name in ADR 0228, and its body."""

    number: int
    name: str
    body: Callable[[MailProviderSubject], None]

    def run(self, subject: MailProviderSubject) -> None:
        self.body(subject)


SENDER_DOMAIN = "sender.example"


def _sender(label: str) -> str:
    return f"{label}-{uuid.uuid4().hex[:8]}@{SENDER_DOMAIN}"


def _require(condition: bool, check: str, expected: str, observed: object) -> None:
    if not condition:
        raise ConformanceFailure(f"{check}: expected {expected}; observed {observed!r}")


def _read(subject: MailProviderSubject, check: str, message_id: str) -> Message:
    message = subject.provider.get_message(message_id)
    _require(isinstance(message, Message), check, "the message to be readable", message)
    assert isinstance(message, Message)
    return message


def _bare(address: str) -> str:
    return email.utils.parseaddr(address)[1].strip().lower()


def _listing(subject: MailProviderSubject, check: str) -> list[str]:
    """Walk the whole listing one item a page and return the ids in order."""
    ids: list[str] = []
    cursors: set[str] = set()
    cursor: str | None = None
    for _ in range(10_000):
        page = subject.provider.list_messages(cursor, 1)
        _require(not isinstance(page, Failure), check, "every page to list", page)
        assert not isinstance(page, Failure)
        ids.extend(summary.message_id for summary in page.messages)
        if page.next_cursor is None:
            return ids
        _require(page.next_cursor not in cursors, check, "no repeated cursor", page.next_cursor)
        cursors.add(page.next_cursor)
        cursor = page.next_cursor
    raise ConformanceFailure(f"{check}: the listing never reached its end")


def check_paging(subject: MailProviderSubject) -> None:
    """1. Paging walks to the end without repeats, and mail this inbox sent is flagged."""
    name = "paging"
    placed = [
        subject.place(sender=_sender("page"), subject="page", new_text="hi") for _ in range(3)
    ]
    sent = subject.place(
        sender=subject.provider.inbox_address, subject="sent", new_text="out", sent_by_inbox=True
    )
    ids = _listing(subject, name)
    _require(len(ids) == len(set(ids)), name, "no message listed twice", ids)
    _require(set(placed) <= set(ids), name, "every placed message listed", ids)
    flagged = {}
    cursor: str | None = None
    while True:
        page = subject.provider.list_messages(cursor, 50)
        assert not isinstance(page, Failure)
        flagged.update({item.message_id: item.sent_by_inbox for item in page.messages})
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    if sent in flagged:
        _require(flagged[sent], name, "a message this inbox sent to be flagged", flagged[sent])
    _require(
        not any(flagged.get(message_id) for message_id in placed),
        name,
        "received mail not flagged as sent",
        flagged,
    )


def check_ids_and_threads(subject: MailProviderSubject) -> None:
    """2. Ids are stable, a reply lands in its parent's thread, and the inbox address is right."""
    name = "ids_and_threads"
    parent = subject.place(sender=_sender("parent"), subject="parent", new_text="first")
    first = _read(subject, name, parent)
    again = _read(subject, name, parent)
    _require(
        (first.message_id, first.thread_id) == (again.message_id, again.thread_id),
        name,
        "the same ids on every read",
        (first, again),
    )
    _require(first.message_id == parent, name, "the read id to be the placed id", first.message_id)
    child = subject.place(
        sender=_sender("child"), subject="Re: parent", new_text="second", reply_to=parent
    )
    _require(
        _read(subject, name, child).thread_id == first.thread_id,
        name,
        "a reply in its parent's thread",
        child,
    )
    inbox = subject.provider.inbox_address
    _require(
        inbox in {_bare(address) for address in first.to},
        name,
        f"mail to the inbox to arrive at {inbox!r}",
        first.to,
    )


def check_text(subject: MailProviderSubject) -> None:
    """3. New text excludes quoted history, the subject is reported, HTML-only has no text."""
    name = "text"
    parent = subject.place(sender=_sender("asker"), subject="Approval", new_text="please")
    reply = subject.place(
        sender=_sender("answerer"),
        subject="Re: Approval",
        new_text="Thanks",
        full_text="Thanks\n\n> APPROVE\n> please",
        reply_to=parent,
    )
    message = _read(subject, name, reply)
    _require(message.subject == "Re: Approval", name, "the subject", message.subject)
    _require(
        message.new_text is None or "APPROVE" not in message.new_text,
        name,
        "new text without the quoted APPROVE",
        message.new_text,
    )
    html_only = subject.place(
        sender=_sender("html"), subject="html", new_text=None, html="<p>only html</p>"
    )
    message = _read(subject, name, html_only)
    _require(
        message.new_text is None and message.text is None,
        name,
        "no text for HTML-only",
        (message.new_text, message.text),
    )
    _require(bool(message.html or message.new_html), name, "an HTML body", message.html)


def check_verdicts(subject: MailProviderSubject) -> None:
    """4. Nothing unverifiable or forged is verified; a verdict names the From domain."""
    name = "verdicts"
    provider = subject.provider
    plain = _read(subject, name, subject.place(sender=_sender("plain"), subject="v", new_text="v"))
    _require(
        plain.verdict.outcome != "verified", name, "no verdict without evidence", plain.verdict
    )
    forged = _read(
        subject,
        name,
        subject.place(
            sender=_sender("forged"), subject="v", new_text="v", forged_authentication=True
        ),
    )
    _require(
        forged.verdict.outcome != "verified",
        name,
        "forged sender-written headers never verify",
        forged.verdict,
    )
    two = _read(
        subject,
        name,
        subject.place(
            sender=_sender("one"), subject="v", new_text="v", extra_senders=(_sender("two"),)
        ),
    )
    _require(len(two.senders) == 2, name, "both From mailboxes reported", two.senders)
    authentic = _read(
        subject,
        name,
        subject.place(sender=_sender("real"), subject="v", new_text="v", authenticated=True),
    )
    if not provider.verifies_senders:
        _require(
            authentic.verdict.outcome == "unverifiable",
            name,
            "a provider that cannot verify to return unverifiable",
            authentic.verdict,
        )
        return
    _require(
        authentic.verdict.outcome == "verified",
        name,
        "an authenticated sender to verify",
        authentic.verdict,
    )
    _require(
        authentic.verdict.domain.lower() == SENDER_DOMAIN,
        name,
        f"the verdict to name the From domain {SENDER_DOMAIN!r}",
        authentic.verdict.domain,
    )


def check_sends(subject: MailProviderSubject) -> None:
    """5. Sender-only and reply-all reach the expected recipients, and the marker is visible."""
    name = "sends"
    provider = subject.provider
    sender = _sender("asker")
    copied = _sender("copied")
    message_id = subject.place(sender=sender, subject="send", new_text="go", cc=(copied,))
    thread_id = _read(subject, name, message_id).thread_id
    marker = f"conformance-marker-{uuid.uuid4().hex}"
    sent = provider.send_reply(message_id, f"reply\n\n{marker}", False)
    _require(not isinstance(sent, Failure), name, "a sender-only reply to be accepted", sent)
    _require(
        subject.recipients_of_last_send() == {sender},
        name,
        f"only {sender}",
        subject.recipients_of_last_send(),
    )
    if provider.sent_visible_within == 0:
        _require(
            provider.thread_contains(thread_id, marker) == "yes",
            name,
            "the marker visible as soon as the send returns",
            marker,
        )
    sent = provider.send_reply(message_id, "reply all", True)
    _require(not isinstance(sent, Failure), name, "a reply-all to be accepted", sent)
    _require(
        subject.recipients_of_last_send() == {sender, copied},
        name,
        f"{sender} and {copied}",
        subject.recipients_of_last_send(),
    )
    _require(
        provider.thread_contains(thread_id, f"absent-{uuid.uuid4().hex}") == "no",
        name,
        "an absent marker reported as no",
        thread_id,
    )


def check_failures(subject: MailProviderSubject) -> None:
    """6. Each failure kind is reported for its own cause; deleted only when definitive."""
    name = "failures"
    provider = subject.provider
    message_id = subject.place(sender=_sender("fail"), subject="f", new_text="f")
    calls: dict[str, Callable[[], object]] = {
        "list": lambda: provider.list_messages(None, 5),
        "get": lambda: provider.get_message(message_id),
        "send": lambda: provider.send_reply(message_id, "x", False),
    }
    for operation, call in calls.items():
        for kind in FAILURE_KINDS:
            if not subject.fail_next(operation, kind):
                continue
            result = call()
            _require(
                isinstance(result, Failure) and result.kind == kind,
                name,
                f"{operation} to report {kind}",
                result,
            )
    thread_id = _read(subject, name, message_id).thread_id
    if subject.fail_next("thread", "server_error"):
        answer = provider.thread_contains(thread_id, "x")
        _require(isinstance(answer, Failure), name, "an ambiguous lookup is not deleted", answer)
    subject.delete_thread(thread_id)
    _require(
        provider.thread_contains(thread_id, "x") == "deleted",
        name,
        "a deleted thread reported as deleted",
        thread_id,
    )


def check_causes(subject: MailProviderSubject) -> None:
    """7. No failure cause contains a credential, and every cause is bounded."""
    name = "causes"
    provider = subject.provider
    for kind in FAILURE_KINDS:
        if not subject.fail_next("list", kind):
            continue
        result = provider.list_messages(None, 5)
        if not isinstance(result, Failure):
            continue
        for credential in subject.credentials():
            _require(
                not credential or credential not in result.cause,
                name,
                "no credential in a failure cause",
                kind,
            )
        _require(
            len(result.cause) <= FAILURE_CAUSE_MAX_CHARS + 3,
            name,
            "a bounded cause",
            len(result.cause),
        )


CHECKS: tuple[Check, ...] = (
    Check(1, "paging", check_paging),
    Check(2, "ids_and_threads", check_ids_and_threads),
    Check(3, "text", check_text),
    Check(4, "verdicts", check_verdicts),
    Check(5, "sends", check_sends),
    Check(6, "failures", check_failures),
    Check(7, "causes", check_causes),
)
