"""A mail provider that can report verified senders, for the neutral adapter's tests.

ADR 0228 decision 5: the neutral adapter's own tests, approvals included, run
against a test provider that can return ``verified``, driving the intake path
and the approval reply path end to end through the real adapter and the fake
platform. Only external services are faked: this provider speaks AgentMail's
HTTP API to the fake AgentMail server in `_support.py`, exactly as the built-in
provider does, and differs in one place. Its verdict comes from
``receiving_verdict``, a field the fake server attaches from
`MailState.verdicts`, which stands for a receiving mail system's own
authentication result: evidence a sender cannot write. Every header in the
message, forged ``Authentication-Results`` included, is ignored.
"""

from __future__ import annotations

from typing import Any

from curie_mail_adapter.agentmail import AgentMailClient, AgentMailProvider
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.provider import UNVERIFIABLE, SenderVerdict


class VerifyingTestProvider(AgentMailProvider):
    """AgentMail's wire, with a sender verdict the fake receiving system supplies."""

    name = "verifying-test"
    verifies_senders = True

    def __init__(
        self,
        config: MailAdapterConfig,
        client: AgentMailClient | None = None,
        *,
        sent_visible_within: float = 0.0,
    ) -> None:
        super().__init__(config, client)
        self.sent_visible_within = sent_visible_within

    def _verdict(self, full: dict[str, Any]) -> SenderVerdict:
        stamped = full.get("receiving_verdict")
        if not isinstance(stamped, dict):
            return UNVERIFIABLE
        outcome = stamped.get("outcome")
        if outcome == "verified":
            return SenderVerdict("verified", domain=str(stamped.get("domain") or ""))
        if outcome == "failed":
            return SenderVerdict("failed", reason=str(stamped.get("reason") or "dmarc=fail"))
        return UNVERIFIABLE


def verified(domain: str) -> dict[str, str]:
    """The fake receiving system's verdict that DMARC passed for ``domain``."""
    return {"outcome": "verified", "domain": domain}


def failed(reason: str = "dmarc=fail") -> dict[str, str]:
    """The fake receiving system's verdict that authentication failed."""
    return {"outcome": "failed", "reason": reason}
