"""The inbound gate: the provider's filtering, the label check, the allow-list.

Two independent controls in one file because they are two halves of one gate,
and the plan is explicit that they are layered rather than equivalent:

1. Provider-side filtering is the primary control and it is not ours. AgentMail
   drops mail whose authentication headers are present and explicitly fail
   before it reaches the API, and excludes the `spam`, `blocked` and
   `unauthenticated` categories from list results by default.
2. The adapter states that exclusion in the request rather than inheriting it,
   so a changed provider default cannot widen an install silently.
3. The `labels` check is defense in depth that should never fire in a correct
   install.

The allow-list is a fourth thing and is NOT part of that chain: it is a filter
on an attacker-controlled `From` header and it authenticates nobody.

The two mutations are independent on purpose: dropping the `include_*`
parameters reddens only the request-shape case, and dropping the label check
reddens only the two label cases.
"""

from __future__ import annotations

import logging
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


def _warnings_about(caplog: pytest.LogCaptureFixture, needle: str) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.WARNING
        and record.name.startswith("curie_mail_adapter")
        and needle in record.getMessage()
    ]


# --- the allow-list -----------------------------------------------------------


def test_an_exact_address_entry_admits_that_sender(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    adapter = make_adapter(allowed_senders=("alice@example.com",))
    mail.add_inbound("msg-1", "thr-1", sender="alice@example.com")

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-1"]


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

    assert ingress.delivery_ids() == (["msg-1"] if admitted else [])


def test_matching_is_case_insensitive_and_tolerates_whitespace(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    """Both the configured entry and the header vary in case in the wild."""
    adapter = make_adapter(allowed_senders=("  Alice@Example.COM ", " OTHER-DOMAIN.com "))
    mail.add_inbound("msg-1", "thr-1", sender="ALICE@example.com")
    mail.add_inbound("msg-2", "thr-2", sender="Bob@Other-Domain.com")

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-1", "msg-2"]


def test_a_display_name_from_header_is_matched_on_the_bare_address(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    """The spike passed `m.get("from")` raw, so a display name defeated the list."""
    adapter = make_adapter(allowed_senders=("alice@example.com",))
    mail.add_inbound("msg-1", "thr-1", sender="Alice Example <alice@example.com>")
    mail.add_inbound("msg-2", "thr-2", sender="Alice Example <mallory@evil.example>")

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-1"]


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
    assert "msg-junk" in adapter.seen
    warnings = _warnings_about(caplog, "msg-junk")
    assert len(warnings) == 1, warnings
    assert STRANGER in warnings[0]

    with caplog.at_level(logging.WARNING):
        adapter.poll_once()

    assert ingress.attempts == 0
    assert len(_warnings_about(caplog, "msg-junk")) == 1  # not re-evaluated, not re-logged


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


def test_allow_all_is_reachable_only_by_writing_the_star(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    """The dangerous state must be named explicitly, never produced by omission."""
    adapter = make_adapter(allowed_senders=("*",))
    mail.add_inbound("msg-1", "thr-1", sender="whoever@wherever.example")

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-1"]


def test_the_allow_list_gates_ingress_only_not_egress(
    mail: MailState,
    make_adapter: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    """Staged cutover: ingress off with no allow-list still serves the reply wire."""
    adapter = make_adapter(ingress_enabled=False, allowed_senders=())
    url = serve_egress(adapter) + "/"
    mail.add_inbound("msg-9", "thr-9")
    # The one internal shape this suite seeds rather than drives: with ingress
    # off there is no path that creates a conversation record, and the plan calls
    # for a manually seeded one here.
    adapter.conversations["thr-9"] = {"text": "the answer"}

    status, _ = post_event(url, completed("ev-off", conversation_id="thr-9", reply_ref="msg-9"))

    assert status == 200
    assert [mid for mid, _text in mail.replies] == ["msg-9"]
    assert "the answer" in mail.replies[0][1]


# --- the provider's filtering, and the label check behind it ------------------


@pytest.mark.parametrize("label", LEAKED_LABELS)
def test_the_label_check_catches_what_a_widened_provider_would_deliver(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    label: str,
) -> None:
    """Defense in depth against a provider default change or a key with label-read
    permissions. In a correct install this path never executes, and it is not what
    makes the install safe: see the module docstring for the real ordering.
    """
    mail.leak_labeled = True  # the provider is serving what it normally withholds
    mail.add_inbound("msg-bad", "thr-bad", sender=ALLOWED_SENDER, labels=[label])

    adapter.poll_once()

    assert ingress.attempts == 0
    assert mail.replies == []
    assert adapter.conversations == {}
    assert "msg-bad" in adapter.seen  # rejected once, not re-evaluated forever


def test_the_label_check_runs_before_the_allow_list(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A labeled message from a stranger is rejected once, naming the label."""
    mail.leak_labeled = True
    mail.add_inbound("msg-bad", "thr-bad", sender=STRANGER, labels=["unauthenticated"])

    with caplog.at_level(logging.WARNING):
        adapter.poll_once()

    assert ingress.attempts == 0
    warnings = _warnings_about(caplog, "msg-bad")
    assert len(warnings) == 1, warnings
    assert "unauthenticated" in warnings[0]


def test_an_empty_labels_array_is_admitted(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """The common case is no labels at all; absence must not read as a rejection."""
    mail.add_inbound("msg-1", "thr-1", labels=[])

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-1"]


def test_a_sent_label_keeps_its_self_echo_meaning(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """`sent` is the adapter's own outbound mail, skipped without a rejection warning."""
    mail.add_inbound("msg-ours", "thr-1", sender=INBOX, labels=["sent"])
    mail.add_inbound("msg-theirs", "thr-1", sender=ALLOWED_SENDER, labels=[])

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-theirs"]


def test_the_list_request_states_the_exclusions_explicitly(
    mail: MailState, adapter: MailAdapter
) -> None:
    """Layer 2: the adapter says no rather than inheriting a default that can move.

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
    """Layer 1, and it is where the protection actually comes from today.

    With the fake behaving as the provider documents, a spam-labeled message from
    an allow-listed sender is never served at all, so the adapter never sees it:
    zero ingress attempts and no entry in `seen`, which is a different outcome
    from the label check's (which marks the message seen).
    """
    mail.add_inbound("msg-spam", "thr-spam", sender=ALLOWED_SENDER, labels=["spam"])
    mail.add_inbound("msg-ok", "thr-ok", sender=ALLOWED_SENDER)

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-ok"]
    assert "msg-spam" not in adapter.seen
