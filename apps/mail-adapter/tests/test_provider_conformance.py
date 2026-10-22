"""The mail provider conformance kit, run against the built-in and the test provider.

ADR 0228 decision 5: AgentMail passes the kit against the existing fake
AgentMail server, and so does the verifying test provider the neutral adapter's
own tests use. A provider that trusts sender-written headers must fail it, or
the kit's verdict check proves nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from _support import AGENTMAIL_API_KEY, INBOX, MailState
from _verifying_provider import VerifyingTestProvider, verified
from curie_mail_adapter.agentmail import AgentMailProvider
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.provider import FailureKind, MailProvider, SenderVerdict
from curie_mail_adapter.provider_conformance import (
    CHECKS,
    SENDER_DOMAIN,
    ConformanceFailure,
    check_verdicts,
)

MAX_BODY_BYTES = 65_536
_STATUS_FOR_KIND: dict[str, int] = {
    "unreachable": 0,
    "refused": 403,
    "rate_limited": 429,
    "server_error": 500,
}


class FakeInboxSubject:
    """The conformance subject over the fake AgentMail server."""

    def __init__(self, mail: MailState, provider: MailProvider) -> None:
        self.mail = mail
        self.provider = provider
        self._count = 0

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
        self._count += 1
        message_id = f"conf-{self._count}"
        thread_id = (
            self.mail.bodies[reply_to]["thread_id"] if reply_to else f"conf-thread-{self._count}"
        )
        headers = {"From": sender}
        if forged_authentication:
            headers["Authentication-Results"] = (
                f"mx.example.com; dmarc=pass header.from={sender.rpartition('@')[2]}"
            )
        self.mail.add_inbound(
            message_id,
            thread_id,
            sender=", ".join((sender, *extra_senders)),
            subject=subject,
            text=new_text,
            full_text=full_text,
            headers=headers,
            cc=list(cc) or None,
            labels=["sent"] if sent_by_inbox else None,
        )
        if html is not None:
            self.mail.bodies[message_id]["html"] = html
        if authenticated:
            self.mail.verdicts[message_id] = verified(SENDER_DOMAIN)
        return message_id

    def recipients_of_last_send(self) -> frozenset[str]:
        return self.mail.deliveries[-1][1]

    def fail_next(self, operation: str, kind: FailureKind) -> bool:
        self.mail.injected_body = None
        if kind == "too_large":
            if operation not in ("list", "get"):
                return False
            # A provider answer larger than the adapter's body budget.
            self.mail.injected_body = {"padding": "x" * (MAX_BODY_BYTES * 2)}
            status = 200
        elif kind in _STATUS_FOR_KIND:
            status = _STATUS_FOR_KIND[kind]
        else:
            return False
        if operation == "list":
            self.mail.fail_next_list = status
        elif operation == "get":
            self.mail.fail_next_body = status
        elif operation == "send":
            self.mail.fail_next_reply = status
        elif operation == "thread":
            self.mail.fail_next_thread = status
        else:
            return False
        return True

    def delete_thread(self, thread_id: str) -> None:
        self.mail.deleted_threads.add(thread_id)

    def credentials(self) -> tuple[str, ...]:
        return (AGENTMAIL_API_KEY,)


class _TrustsForgedHeaders(VerifyingTestProvider):
    """A broken provider: it believes an Authentication-Results header a sender wrote."""

    def _verdict(self, full: dict[str, Any]) -> SenderVerdict:
        claimed = str((full.get("headers") or {}).get("Authentication-Results") or "")
        if "dmarc=pass" in claimed:
            return SenderVerdict("verified", domain=claimed.rpartition("header.from=")[2])
        return super()._verdict(full)


@pytest.fixture
def conformance_config(make_config: Callable[..., MailAdapterConfig]) -> MailAdapterConfig:
    return make_config(max_body_bytes=MAX_BODY_BYTES)


@pytest.mark.parametrize("check", CHECKS, ids=[check.name for check in CHECKS])
@pytest.mark.parametrize("provider_name", ["agentmail", "verifying-test"])
def test_each_provider_passes_each_conformance_check(
    mail: MailState,
    conformance_config: MailAdapterConfig,
    provider_name: str,
    check: Any,
) -> None:
    provider: MailProvider = (
        AgentMailProvider(conformance_config)
        if provider_name == "agentmail"
        else VerifyingTestProvider(conformance_config)
    )
    assert provider.inbox_address == INBOX
    check.run(FakeInboxSubject(mail, provider))


def test_the_verdict_check_fails_a_provider_that_trusts_sender_written_headers(
    mail: MailState, conformance_config: MailAdapterConfig
) -> None:
    subject = FakeInboxSubject(mail, _TrustsForgedHeaders(conformance_config))

    with pytest.raises(ConformanceFailure, match="forged sender-written headers never verify"):
        check_verdicts(subject)
