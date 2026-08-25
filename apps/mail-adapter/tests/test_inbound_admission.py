"""Inbound admission ordering and durable-store failure behavior."""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest
from _support import ALLOWED_SENDER, STRANGER, IngressState, MailState
from curie_mail_adapter.adapter import MailAdapter


@pytest.mark.parametrize("gate", ["provider-label", "sender-allow-list"])
def test_rejected_listings_persist_only_a_minimal_receipt_without_spending_capacity(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    gate: str,
) -> None:
    """Untrusted listing metadata must not become durable mailbox storage.

    Rejected receipts remain durable so the same provider id is not reconsidered
    forever, but their sender/subject listing PII must be discarded before the
    active-item and byte budgets are charged.  A legitimate message therefore
    remains admissible after a burst of large rejected listings.
    """
    adapter = make_adapter(
        max_pending_deliveries=1,
        max_state_bytes=128 * 1024,
    )
    rejected_ids = [f"msg-rejected-{index}" for index in range(8)]
    private_marker = "private-listing-metadata-" + ("x" * (24 * 1024))
    if gate == "provider-label":
        mail.leak_labeled = True

    for message_id in rejected_ids:
        mail.add_inbound(
            message_id,
            f"thr-{message_id}",
            sender=ALLOWED_SENDER if gate == "provider-label" else STRANGER,
            subject=private_marker,
            labels=["unauthenticated"] if gate == "provider-label" else [],
        )

    adapter.poll_once()

    rows = adapter.state.connection.execute(
        "SELECT message_id, state, summary_json, turn_json FROM deliveries "
        "WHERE message_id LIKE 'msg-rejected-%' ORDER BY message_id"
    ).fetchall()
    assert len(rows) == len(rejected_ids)
    for message_id, state, summary_json, turn_json in rows:
        assert state == "rejected"
        assert turn_json is None
        minimal_summary = json.loads(summary_json) if summary_json else {}
        assert set(minimal_summary) <= {"message_id"}
        assert minimal_summary.get("message_id", message_id) == message_id
        assert private_marker not in (summary_json or "")
        assert STRANGER not in (summary_json or "")

    mail.add_inbound("msg-legitimate", "thr-legitimate", sender=ALLOWED_SENDER)
    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-legitimate"]
    assert mail.body_calls == {"msg-legitimate": 1}


def test_sent_listing_is_discarded_before_any_durable_claim_or_body_fetch(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
) -> None:
    """The adapter's provider-side self echo is never an inbound delivery."""
    mail.add_inbound(
        "msg-self-echo",
        "thr-self-echo",
        subject="private outbound subject",
        labels=["sent"],
    )

    adapter.poll_once()

    assert adapter.state.delivery("msg-self-echo") is None
    assert mail.body_calls == {}
    assert ingress.attempts == 0


@pytest.mark.parametrize("fault", ["full", "readonly"])
def test_sqlite_admission_failure_refuses_without_crashing_or_burning_mail(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    fault: str,
) -> None:
    """A local disk fault is back-pressure, not message acknowledgement.

    The fault is induced through SQLite itself.  Once storage is writable again,
    the untouched provider delivery must traverse the ordinary body and ingress
    path exactly once.
    """
    adapter = make_adapter()
    mail.add_inbound(
        "msg-disk-pressure",
        "thr-disk-pressure",
        subject="large-enough-to-require-new-pages-" + ("x" * (128 * 1024)),
    )
    connection = adapter.state.connection
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])

    if fault == "full":
        connection.execute(f"PRAGMA max_page_count={page_count}")
        # Bypass the adapter's proactive byte guard so the real SQLite FULL path
        # is exercised by the oversized INSERT rather than by a synthetic mock.
        adapter.state.max_bytes = (page_count + 256) * page_size
    else:
        connection.execute("PRAGMA query_only=ON")

    assert adapter.poll_once() == 200
    assert "msg-disk-pressure" not in adapter.seen
    assert adapter.state.delivery("msg-disk-pressure") is None
    assert mail.body_calls == {}
    assert ingress.attempts == 0

    if fault == "full":
        connection.execute(f"PRAGMA max_page_count={page_count + 256}")
    else:
        connection.execute("PRAGMA query_only=OFF")
    adapter.state.max_bytes = (page_count + 256) * page_size

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-disk-pressure"]
    assert mail.body_calls == {"msg-disk-pressure": 1}


def test_prime_ignores_a_malformed_listing_without_crashing_startup(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
) -> None:
    """One malformed provider item cannot keep the replacement unready."""
    mail.add_inbound("msg-existing", "thr-existing")
    mail.messages.append(
        {
            "thread_id": "thr-malformed",
            "from": ALLOWED_SENDER,
            "subject": "provider omitted the message id",
            "labels": [],
        }
    )

    adapter.startup()

    assert adapter.ready.is_set()
    assert adapter.state.is_primed()
    assert adapter.state.known_message_ids() == ["msg-existing"]
    assert adapter.state.delivery("msg-existing") == {"state": "primed", "turn": None}
    assert mail.body_calls == {}
    assert ingress.attempts == 0


def test_primed_receipts_survive_the_terminal_cap_and_reopen(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
) -> None:
    """Compaction must never turn first-boot mailbox history into new turns."""
    state_bytes = 128 * 1024
    first = make_adapter(max_state_bytes=state_bytes)
    reopened: MailAdapter | None = None
    try:
        # The public byte limit deterministically derives the smallest receipt
        # cap.  Seed one more allowed message to cross that boundary without
        # mutating the store's internal policy in the test.
        receipt_cap = first.state.terminal_receipt_max
        assert receipt_cap == 8
        preexisting_ids = [f"msg-preexisting-{index}" for index in range(receipt_cap + 1)]
        for message_id in preexisting_ids:
            mail.add_inbound(message_id, f"thr-{message_id}", sender=ALLOWED_SENDER)

        first.startup()
        first.poll_once()

        assert ingress.delivery_ids() == []
        assert mail.body_calls == {}
        first.close()

        reopened = make_adapter(max_state_bytes=state_bytes)
        reopened.startup()
        reopened.poll_once()

        assert ingress.delivery_ids() == []
        assert mail.body_calls == {}
        assert set(reopened.state.known_message_ids()) >= set(preexisting_ids)
    finally:
        if reopened is not None:
            reopened.close()
        elif first.state.healthy():
            first.close()
