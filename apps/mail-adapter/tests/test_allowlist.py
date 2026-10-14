"""Sender matching is only a filter; all AgentMail authentication fails closed.

Provider list exclusions and rejection labels remain useful mailbox controls.
Neither a matching From address nor absent labels establishes authentication.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable

import pytest
from _support import (
    AGENTMAIL_API_KEY,
    ALLOWED_SENDER,
    INBOX,
    STRANGER,
    IngressState,
    MailState,
    completed,
    post_event,
)
from curie_mail_adapter.adapter import MailAdapter

LEAKED_LABELS = ["unauthenticated", "spam", "blocked"]


def _adapter_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.WARNING and record.name.startswith("curie_mail_adapter")
    ]


# --- the allow-list -----------------------------------------------------------


def test_an_exact_address_match_still_cannot_authenticate_that_sender(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    adapter = make_adapter(allowed_senders=("alice@example.com",))
    mail.add_inbound("msg-1", "thr-1", sender="alice@example.com")

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.sender_allowed("alice@example.com")


@pytest.mark.parametrize(
    ("sender", "admitted"),
    [
        ("anyone@that-domain.com", True),
        ("anyone@other-domain.com", False),
        ("anyone@sub.that-domain.com", False),  # no subdomain matching
    ],
)
def test_a_bare_domain_entry_matches_only_that_domain(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    sender: str,
    admitted: bool,
) -> None:
    adapter = make_adapter(allowed_senders=("that-domain.com",))
    mail.add_inbound("msg-1", "thr-1", sender=sender)

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.sender_allowed(sender) is admitted


def test_matching_is_case_insensitive_and_tolerates_whitespace(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    """Both the configured entry and the header vary in case in the wild."""
    adapter = make_adapter(allowed_senders=("  Alice@Example.COM ", " OTHER-DOMAIN.com "))
    mail.add_inbound("msg-1", "thr-1", sender="ALICE@example.com")
    mail.add_inbound("msg-2", "thr-2", sender="Bob@Other-Domain.com")

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.sender_allowed("ALICE@example.com")
    assert adapter.sender_allowed("Bob@Other-Domain.com")


def test_a_display_name_from_header_is_matched_on_the_bare_address(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    """The spike passed `m.get("from")` raw, so a display name defeated the list."""
    adapter = make_adapter(allowed_senders=("alice@example.com",))
    mail.add_inbound("msg-1", "thr-1", sender="Alice Example <alice@example.com>")
    mail.add_inbound("msg-2", "thr-2", sender="Alice Example <mallory@evil.example>")

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.sender_allowed("Alice Example <alice@example.com>")
    assert not adapter.sender_allowed("Alice Example <mallory@evil.example>")


def test_a_rejected_sender_posts_nothing_and_is_marked_seen(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The drop is permanent, and that is the decision this test pins.

    An unmarked rejection is re-evaluated and re-logged every poll interval
    forever, turning one unwanted email into an unbounded log flood and a
    permanent slot in the listing window.
    """
    mail.add_inbound("msg-junk", "thr-junk", sender=STRANGER)

    with caplog.at_level(logging.WARNING):
        adapter.poll_once()

    assert ingress.attempts == 0
    assert mail.body_calls == {}, "the allow-list must run before the body GET"
    assert "msg-junk" in adapter.seen
    warnings = _adapter_warnings(caplog)
    assert len(warnings) == 1, warnings
    warning = warnings[0]
    assert "rejected" in warning
    assert "authentication_unverifiable" in warning
    assert re.search(r"\bcorrelation=[0-9a-f]{16}\b", warning)
    assert "msg-junk" not in warning
    assert STRANGER not in warning

    with caplog.at_level(logging.WARNING):
        adapter.poll_once()

    assert ingress.attempts == 0
    assert _adapter_warnings(caplog) == warnings  # not re-evaluated or re-logged


def test_a_rejected_sender_gets_no_reply_and_has_no_conversation_record(
    mail: MailState,
    adapter: MailAdapter,
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    """The allow-list transitively protects egress, and that is load-bearing.

    `conversations` is written only after the inbound checks pass, so a forged or
    replayed `turn.completed` naming a rejected sender's thread finds no record
    and sends nothing. Pre-seeding conversations from the poll listing would
    silently remove this protection.

    The ack is 502 rather than 200 for the reason given on
    `test_a_completion_for_an_unknown_conversation_is_a_retryable_failure`: the
    adapter cannot tell a forged conversation_id from one a restart erased, so
    the missing record is always answered as a delivery failure. Nothing is sent
    either way, which is what this test is here for.
    """
    url = serve_egress(adapter) + "/"
    mail.add_inbound("msg-junk", "thr-junk", sender=STRANGER)

    adapter.poll_once()

    assert mail.replies == []

    status, _ = post_event(
        url, completed("ev-forged", conversation_id="thr-junk", reply_ref="msg-junk")
    )

    assert status == 502
    assert mail.replies == []


def test_a_wildcard_filter_does_not_authenticate_any_sender(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    """The dangerous state must be named explicitly, never produced by omission."""
    adapter = make_adapter(allowed_senders=("*",), allow_all_senders=True)
    mail.add_inbound("msg-1", "thr-1", sender="whoever@wherever.example")

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.sender_allowed("whoever@wherever.example")


def test_ingress_disabled_does_not_authorize_an_unadmitted_egress_ref(
    mail: MailState,
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    """The server stays up during cutover, but only admitted reply refs may send."""
    adapter = make_adapter(ingress_enabled=False, allowed_senders=())
    url = serve_egress(adapter) + "/"
    mail.add_inbound("msg-9", "thr-9")

    status, _ = post_event(url, completed("ev-off", conversation_id="thr-9", reply_ref="msg-9"))

    assert status == 502
    assert mail.replies == []


# --- the provider's filtering, and the label check behind it ------------------


@pytest.mark.parametrize("label", LEAKED_LABELS)
def test_labeled_mail_is_rejected_even_when_provider_filtering_widens(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    label: str,
) -> None:
    """Mail remains unverified when the provider serves normally withheld labels."""
    mail.leak_labeled = True  # the provider is serving what it normally withholds
    mail.add_inbound("msg-bad", "thr-bad", sender=ALLOWED_SENDER, labels=[label])

    adapter.poll_once()

    assert ingress.attempts == 0
    assert mail.body_calls == {}, "authentication must be checked before the body GET"
    assert mail.replies == []
    assert adapter.state.reply_text("thr-bad", "msg-bad") == (False, None)
    assert "msg-bad" in adapter.seen  # rejected once, not re-evaluated forever


def test_unverifiable_authentication_is_named_before_the_sender_filter(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A labeled message from a stranger is rejected once, naming authentication."""
    mail.leak_labeled = True
    mail.add_inbound("msg-bad", "thr-bad", sender=STRANGER, labels=["unauthenticated"])

    with caplog.at_level(logging.WARNING):
        adapter.poll_once()

    assert ingress.attempts == 0
    assert mail.body_calls == {}, "authentication must precede body GET"
    assert "msg-bad" in adapter.seen  # permanent rejection, not re-evaluated forever
    warnings = _adapter_warnings(caplog)
    assert len(warnings) == 1, warnings
    warning = warnings[0]
    assert "rejected" in warning
    assert "authentication_unverifiable" in warning
    assert re.search(r"\bcorrelation=[0-9a-f]{16}\b", warning)
    assert "msg-bad" not in warning
    assert STRANGER not in warning


def test_an_empty_labels_array_still_has_no_authentication_verdict(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """Absence of provider rejection labels cannot establish authentication."""
    mail.add_inbound("msg-1", "thr-1", labels=[])

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.sender_allowed(ALLOWED_SENDER)


def test_a_sent_label_keeps_its_self_echo_meaning(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """`sent` is the adapter's own outbound mail, skipped without a rejection warning."""
    mail.add_inbound("msg-ours", "thr-1", sender=INBOX, labels=["sent"])
    mail.add_inbound("msg-theirs", "thr-1", sender=ALLOWED_SENDER, labels=[])

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.state.delivery("msg-ours") is None
    assert adapter.state.delivery("msg-theirs") == {"state": "rejected", "turn": None}
    assert mail.body_calls.get("msg-ours", 0) == 0


def test_the_list_request_states_the_exclusions_explicitly(
    mail: MailState, adapter: MailAdapter
) -> None:
    """The adapter states provider exclusions rather than inheriting defaults.

    Parameter names and semantics ("Include <category> in results") are from
    https://docs.agentmail.to/api-reference/inboxes/messages/list ; the documented
    default exclusion they restate is from https://www.agentmail.to/docs/messages .
    Sending them changes nothing about what a correct provider returns today, and
    that is the point: a provider that changes a default, or a key that carries the
    label-read permissions, cannot silently widen what reaches the agent.
    """
    adapter.prime()
    adapter.poll_once()

    assert len(mail.list_queries) == 2, "both the priming call and the poll call are listings"
    for query in mail.list_queries:
        assert query.get("include_spam") == "false"
        assert query.get("include_blocked") == "false"
        assert query.get("include_unauthenticated") == "false"
        assert int(query["limit"]) > 0
    # Bearer auth on every provider call:
    # https://docs.agentmail.to/api-reference/overview
    assert mail.list_authorization == [f"Bearer {AGENTMAIL_API_KEY}"] * 2


def test_the_providers_default_filtering_is_what_keeps_labeled_mail_out(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """Provider filtering reduces listings without granting sender identity.

    With the fake behaving as the provider documents, a spam-labeled message from
    an allow-listed sender is never served at all, so the adapter never sees it:
    zero ingress attempts and no entry in `seen`, which is a different outcome
    from the shared authentication gate (which records a rejection receipt).
    """
    mail.add_inbound("msg-spam", "thr-spam", sender=ALLOWED_SENDER, labels=["spam"])
    mail.add_inbound("msg-ok", "thr-ok", sender=ALLOWED_SENDER)

    adapter.poll_once()

    assert ingress.delivery_ids() == []
    assert adapter.state.delivery("msg-ok") == {"state": "rejected", "turn": None}
    assert "msg-spam" not in adapter.seen
