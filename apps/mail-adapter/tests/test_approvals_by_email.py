"""Approval ingress refuses unverifiable mail; historical egress remains deliverable.

Turns accepted before upgrade and answered references are persisted in real SQLite
for independent egress coverage. New answers always cross the real provider
HTTP and shared authentication gate, and cannot resolve an approval.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Callable
from typing import Any

import pytest
from _support import (
    ALLOWED_SENDER,
    IngressState,
    MailState,
    approval_card,
    completed,
    post_event,
    reply_post,
    seed_historical_reply,
    settled_card,
    update,
)
from curie_mail_adapter.adapter import (
    APPROVAL_CARD_REF_PREFIX,
    APPROVAL_INSTRUCTIONS,
    APPROVAL_REF_PATTERN,
    MailAdapter,
)

PRINCIPAL = "adp.test-payload.test-signature"


COPIED = "copied@example.com"


REQUESTER = ALLOWED_SENDER


APPROVER = "approver@example.com"


SECOND_APPROVER = "second.approver@example.com"


HUMAN_HEADERS = {"From": ALLOWED_SENDER, "Message-ID": "<reply@example.com>"}


CARD_REF = f"{APPROVAL_CARD_REF_PREFIX}appr-1"


@pytest.fixture
def approvals_adapter(make_adapter: Callable[..., MailAdapter]) -> Any:
    instance = make_adapter(
        adapter_principal=PRINCIPAL,
        allowed_senders=(ALLOWED_SENDER, COPIED, APPROVER, SECOND_APPROVER),
    )
    yield instance
    instance.shutdown.set()


@pytest.fixture
def url(approvals_adapter: MailAdapter, serve_egress: Callable[[MailAdapter], str]) -> str:
    return serve_egress(approvals_adapter) + "/"


def _ask(mail: MailState, adapter: MailAdapter, url: str) -> str:
    """Render an approval request for an asking turn persisted before upgrade."""

    seed_historical_reply(mail, adapter.state, "msg-1", "thr-1", text="Please send the quote")
    assert post_event(url, update("Awaiting approval (appr-1): Send the quote"))[0] == 200
    status, ack = post_event(url, approval_card("appr-1"))
    assert (status, ack) == (200, {"ref": CARD_REF})
    assert post_event(url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    assert APPROVAL_INSTRUCTIONS in request_email
    (reference,) = APPROVAL_REF_PATTERN.findall(request_email)
    return reference


def _reply(
    mail: MailState,
    adapter: MailAdapter,
    message_id: str,
    new_text: str | None,
    *,
    reference: str,
    sender: str = ALLOWED_SENDER,
    headers: dict[str, str] | None = HUMAN_HEADERS,
    quoted: str = "APPROVE",
) -> None:
    """A reply in the thread: new text on top, the request quoted below it."""

    full = f"{new_text or ''}\n\n> {quoted}\n> Approval reference: {reference}"
    mail.add_inbound(
        message_id, "thr-1", sender=sender, text=new_text, full_text=full, headers=headers
    )
    adapter.poll_once()


def _notices(mail: MailState, message_id: str) -> list[str]:
    return mail.replies_to(message_id)


def test_expiry_sends_the_expired_follow_up_and_spends_the_reference(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    assert post_event(url, settled_card(CARD_REF, decision=None))[0] == 200
    assert mail.replies_to("msg-1")[-1] == "This approval expired before anyone answered it."

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference)
    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == []


@pytest.mark.parametrize("label", ["unauthenticated", "spam", "blocked"])
def test_a_listed_address_the_inbound_gate_did_not_verify_is_never_carried(
    mail: MailState,
    ingress: IngressState,
    approvals_adapter: MailAdapter,
    url: str,
    label: str,
) -> None:
    """A listed From address and provider labels cannot establish authentication.
    The shared gate refuses the message before fetching or resolving anything.
    """

    reference = _ask(mail, approvals_adapter, url)
    mail.leak_labeled = True
    full = f"APPROVE\n\n> Approval reference: {reference}"
    mail.add_inbound(
        "msg-2",
        "thr-1",
        sender=ALLOWED_SENDER,
        text="APPROVE",
        full_text=full,
        headers=HUMAN_HEADERS,
        labels=[label],
    )
    approvals_adapter.poll_once()

    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == []
    assert ingress.delivery_ids() == []


def test_a_sender_the_mailbox_does_not_admit_is_refused_before_any_approval_logic(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """ADR-0177 amendment A2, step 2: the inbound allowlist comes before the
    reference, the reply rules and the approver list."""

    reference = _ask(mail, approvals_adapter, url)

    _reply(
        mail,
        approvals_adapter,
        "msg-2",
        "APPROVE",
        reference=reference,
        sender="stranger@example.net",
    )

    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == []
    assert ingress.delivery_ids() == []


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param({**HUMAN_HEADERS, "Auto-Submitted": "auto-replied"}, id="auto-submitted"),
        pytest.param({**HUMAN_HEADERS, "X-Autoreply": "yes"}, id="x-autoreply"),
        pytest.param({**HUMAN_HEADERS, "Precedence": "bulk"}, id="precedence-bulk"),
        pytest.param(
            {**HUMAN_HEADERS, "Content-Type": "multipart/report; report-type=delivery-status"},
            id="bounce",
        ),
        pytest.param(None, id="no-headers-fails-closed"),
    ],
)
def test_an_automatic_reply_is_ignored_without_a_response(
    mail: MailState,
    ingress: IngressState,
    approvals_adapter: MailAdapter,
    url: str,
    headers: dict[str, str] | None,
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference, headers=headers)

    assert ingress.resolves == []
    # No answer back either: responding to software invites a mail loop.
    assert _notices(mail, "msg-2") == []
    assert ingress.delivery_ids() == []


def test_without_an_adapter_principal_a_card_is_plain_text_as_before(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    egress_url: str,
) -> None:
    seed_historical_reply(mail, adapter.state, "msg-1", "thr-1", text="Please send the quote")
    status, ack = post_event(egress_url, approval_card("appr-1"))
    assert (status, ack) == (200, {"ref": None})
    assert post_event(egress_url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    assert APPROVAL_INSTRUCTIONS not in request_email
    assert not re.search(APPROVAL_REF_PATTERN, request_email)

    mail.add_inbound("msg-2", "thr-1", text="APPROVE", full_text="APPROVE", headers=HUMAN_HEADERS)
    adapter.poll_once()
    assert ingress.resolves == []
    assert ingress.delivery_ids() == []


def test_a_redelivered_card_keeps_its_one_reference(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    seed_historical_reply(
        mail, approvals_adapter.state, "msg-1", "thr-1", text="Please send the quote"
    )
    assert post_event(url, approval_card("appr-1"))[0] == 200
    assert post_event(url, approval_card("appr-1"))[0] == 200
    assert post_event(url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    assert len(set(APPROVAL_REF_PATTERN.findall(request_email))) == 1


def test_a_lost_answer_response_still_gets_its_follow_up_and_the_resumed_reply(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """The platform took the first answer but its response was lost: the retry
    reads 409. The card's settlement must still send the follow-up and reopen
    the asking reply, or the resumed turn has nowhere to answer."""

    reference = _ask(mail, approvals_adapter, url)
    _historical_answer(
        mail, approvals_adapter, "msg-2", "APPROVE", reference=reference, sender=ALLOWED_SENDER
    )

    assert post_event(url, settled_card(CARD_REF, decision="approved"))[0] == 200
    assert mail.received_by(ALLOWED_SENDER)[-1].startswith("This request was approved")
    assert post_event(url, update("Sent the quote.", reply_ref="msg-1"))[0] == 200
    assert post_event(url, completed("ev-2"))[0] == 200
    assert mail.replies_to("msg-1")[-1].startswith("Sent the quote.")


def test_a_resumed_turn_that_posts_after_the_card_answers_the_asking_message(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """ADR-0179 decision 3: the resumed turn drops the replayed placeholder, so
    its first delivery names no ref. The ack names the asking message, the
    worker keeps the turn there, and the answer is mailed in the same thread."""

    _ask(mail, approvals_adapter, url)
    assert post_event(url, settled_card(CARD_REF, decision="approved"))[0] == 200

    status, ack = post_event(url, update("Sent the", reply_ref=None))
    assert (status, ack) == (200, {"ref": "msg-1"})
    assert post_event(url, update("Sent the quote.", reply_ref=ack["ref"]))[0] == 200
    assert post_event(url, completed("ev-2", reply_ref=ack["ref"]))[0] == 200
    assert mail.replies_to("msg-1")[-1].startswith("Sent the quote.")


def test_a_ref_less_post_is_acked_with_the_reply_owner_it_landed_on(
    mail: MailState, approvals_adapter: MailAdapter, url: str
) -> None:
    seed_historical_reply(
        mail, approvals_adapter.state, "msg-1", "thr-1", text="Please send the quote"
    )
    assert post_event(url, reply_post("A note from the platform."))[1] == {"ref": "msg-1"}


def test_concurrent_settlements_send_one_follow_up(
    mail: MailState, approvals_adapter: MailAdapter, url: str
) -> None:
    _ask(mail, approvals_adapter, url)
    mail.hold_replies()
    first: list[int] = []
    sender = threading.Thread(
        target=lambda: first.append(post_event(url, settled_card(CARD_REF, decision=None))[0])
    )
    sender.start()
    assert mail.reply_entered.wait(10)

    # The second delivery finds the send claimed and does not send again.
    assert post_event(url, settled_card(CARD_REF, decision=None))[0] == 200
    mail.release_replies()
    sender.join(10)

    assert first == [200]
    assert mail.replies_to("msg-1").count("This approval expired before anyone answered it.") == 1


def test_a_failed_follow_up_is_retried_by_the_next_settlement(
    mail: MailState, approvals_adapter: MailAdapter, url: str
) -> None:
    _ask(mail, approvals_adapter, url)
    mail.fail_next_reply = 503

    assert post_event(url, settled_card(CARD_REF, decision=None))[0] == 502
    assert post_event(url, settled_card(CARD_REF, decision=None))[0] == 200
    assert mail.replies_to("msg-1")[-1] == "This approval expired before anyone answered it."


def _ask_listed(
    mail: MailState,
    adapter: MailAdapter,
    url: str,
    approvers: list[str],
    *,
    cc: list[str] | None = None,
) -> tuple[str, str]:
    """Run one turn to the pause with a card naming ``approvers``.

    Returns the reference and the request email.
    """

    seed_historical_reply(
        mail, adapter.state, "msg-1", "thr-1", text="Please send the quote", cc=cc
    )
    assert post_event(url, update("Awaiting approval (appr-1): Send the quote"))[0] == 200
    card = approval_card("appr-1", requested_by=REQUESTER, approvers=approvers)
    assert post_event(url, card) == (200, {"ref": CARD_REF})
    assert post_event(url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    (reference,) = APPROVAL_REF_PATTERN.findall(request_email)
    return reference, request_email


def _historical_answer(
    mail: MailState,
    adapter: MailAdapter,
    message_id: str,
    new_text: str,
    *,
    reference: str,
    sender: str,
    cc: list[str] | None = None,
) -> None:
    """Persist a winning answer from before upgrade for settlement tests."""

    full = f"{new_text}\n\n> Approval reference: {reference}"
    mail.add_inbound(
        message_id,
        "thr-1",
        sender=sender,
        text=new_text,
        full_text=full,
        headers={"From": sender, "Message-ID": f"<{message_id}@example.com>"},
        cc=cc,
    )
    adapter.state.record_approval_answer(reference, message_id, [sender, *(cc or [])])
    adapter.state.set_approval_ref_state(reference, "answered")


def _follow_ups(mail: MailState, address: str) -> list[str]:
    return [text for text in mail.received_by(address) if text.startswith("This request was")]


def test_a_requester_off_the_list_is_told_who_can_approve_and_to_copy_them_in(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """Nobody on the thread can approve: the email says so, names every listed
    address, and asks for one or more of them, never only one. Nobody else is
    mailed: bringing an approver in is the requester's choice."""

    _reference, request_email = _ask_listed(
        mail, approvals_adapter, url, [APPROVER, SECOND_APPROVER]
    )

    assert "Nobody on this thread can approve this request yet." in request_email
    assert f"Only these addresses can approve it: {APPROVER}, {SECOND_APPROVER}." in request_email
    assert "Reply all to this email and add one or more of them, as many as you like." in (
        request_email
    )
    assert "Anyone listed who is on the thread can then answer by replying all" in request_email
    assert mail.received_by(REQUESTER) == [request_email]
    assert mail.received_by(APPROVER) == []
    assert mail.received_by(SECOND_APPROVER) == []


def test_one_listed_approver_is_named_on_its_own(
    mail: MailState, approvals_adapter: MailAdapter, url: str
) -> None:
    _reference, request_email = _ask_listed(mail, approvals_adapter, url, [APPROVER])
    assert (
        f"Only {APPROVER} can approve it. Reply all to this email and add {APPROVER}."
        in request_email
    )


def test_historical_first_answer_keeps_its_settlement_recipients(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """Stored historical answer participants receive the platform settlement once."""

    reference, _request = _ask_listed(mail, approvals_adapter, url, [APPROVER, SECOND_APPROVER])

    assert mail.replies_to("msg-2") == []

    _historical_answer(
        mail,
        approvals_adapter,
        "msg-3",
        "REJECT\nNot this quarter.",
        reference=reference,
        sender=SECOND_APPROVER,
        cc=[REQUESTER, APPROVER],
    )
    _historical_answer(
        mail,
        approvals_adapter,
        "msg-4",
        "APPROVE",
        reference=reference,
        sender=APPROVER,
        cc=[REQUESTER, SECOND_APPROVER],
    )

    assert mail.replies_to("msg-4") == []
    assert "This approval has already been answered." not in mail.received_by(REQUESTER)

    assert (
        post_event(
            url,
            settled_card(CARD_REF, decision="rejected", resolver=SECOND_APPROVER, note="Not now"),
        )[0]
        == 200
    )
    for person in (REQUESTER, APPROVER, SECOND_APPROVER):
        assert _follow_ups(mail, person) == [
            f"This request was rejected by {SECOND_APPROVER}.\n\nNote: Not now"
        ], person
    assert ingress.delivery_ids() == []


def test_historical_reply_all_answer_settlement_and_resume(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """Stored historical answer participants receive the platform settlement once."""

    reference, _request = _ask_listed(mail, approvals_adapter, url, [APPROVER])
    _historical_answer(
        mail,
        approvals_adapter,
        "msg-3",
        "APPROVE",
        reference=reference,
        sender=APPROVER,
        cc=[REQUESTER],
    )

    assert post_event(url, settled_card(CARD_REF, decision="approved", resolver=APPROVER))[0] == 200
    assert _follow_ups(mail, REQUESTER) == [f"This request was approved by {APPROVER}."]
    assert _follow_ups(mail, APPROVER) == [f"This request was approved by {APPROVER}."]

    assert post_event(url, update("Sent the quote.", reply_ref="msg-1"))[0] == 200
    assert post_event(url, completed("ev-2"))[0] == 200
    assert mail.received_by(REQUESTER)[-1].startswith("Sent the quote.")


def test_historical_sender_only_answer_still_settles_to_the_requester(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """Stored historical answer participants receive the platform settlement once."""

    reference, _request = _ask_listed(mail, approvals_adapter, url, [APPROVER])
    _historical_answer(
        mail, approvals_adapter, "msg-3", "APPROVE", reference=reference, sender=APPROVER
    )

    settle = settled_card(CARD_REF, decision="approved", resolver=APPROVER)
    mail.fail_next_reply_to = "msg-1"
    assert post_event(url, settle)[0] == 502
    assert post_event(url, settle)[0] == 200
    assert post_event(url, settle)[0] == 200

    assert _follow_ups(mail, APPROVER) == [f"This request was approved by {APPROVER}."]
    assert _follow_ups(mail, REQUESTER) == [f"This request was approved by {APPROVER}."]

    assert post_event(url, update("Sent the quote.", reply_ref="msg-1"))[0] == 200
    assert post_event(url, completed("ev-2"))[0] == 200
    assert mail.received_by(REQUESTER)[-1].startswith("Sent the quote.")
    assert not mail.received_by(APPROVER)[-1].startswith("Sent the quote.")


def test_historical_requester_answer_settlement(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """Stored historical answer participants receive the platform settlement once."""

    reference, request_email = _ask_listed(mail, approvals_adapter, url, [REQUESTER, APPROVER])
    assert f"Who can approve: {REQUESTER}, {APPROVER}." in request_email
    assert f"Already on this thread and able to answer: {REQUESTER}." in request_email
    assert "Nobody on this thread" not in request_email

    _historical_answer(
        mail, approvals_adapter, "msg-2", "APPROVE", reference=reference, sender=REQUESTER
    )
    assert (
        post_event(url, settled_card(CARD_REF, decision="approved", resolver=REQUESTER))[0] == 200
    )
    assert _follow_ups(mail, REQUESTER) == [f"This request was approved by {REQUESTER}."]
    assert mail.received_by(APPROVER) == []


def test_historical_copied_approver_gets_request_and_settlement_only(
    mail: MailState, approvals_adapter: MailAdapter, url: str
) -> None:
    """Stored historical answer participants receive the platform settlement once."""

    reference, request_email = _ask_listed(
        mail, approvals_adapter, url, [APPROVER, SECOND_APPROVER], cc=[APPROVER]
    )
    assert f"Already on this thread and able to answer: {APPROVER}." in request_email
    assert mail.received_by(APPROVER) == [request_email]
    assert mail.received_by(REQUESTER) == [request_email]
    assert mail.received_by(SECOND_APPROVER) == []

    _historical_answer(
        mail,
        approvals_adapter,
        "msg-2",
        "APPROVE",
        reference=reference,
        sender=APPROVER,
        cc=[REQUESTER],
    )
    assert post_event(url, settled_card(CARD_REF, decision="approved", resolver=APPROVER))[0] == 200
    assert post_event(url, update("Sent the quote.", reply_ref="msg-1"))[0] == 200
    assert post_event(url, completed("ev-2"))[0] == 200
    assert mail.received_by(REQUESTER)[-1].startswith("Sent the quote.")
    assert not any(text.startswith("Sent the quote.") for text in mail.received_by(APPROVER))


def test_when_the_asking_message_cannot_be_read_the_request_holds_either_way(
    mail: MailState, approvals_adapter: MailAdapter, url: str
) -> None:
    """The adapter reads the asking message's To and Cc when the card arrives.
    If the provider will not serve it, the request does not claim that nobody
    on the thread can approve."""

    seed_historical_reply(
        mail, approvals_adapter.state, "msg-1", "thr-1", text="Please send the quote", cc=[APPROVER]
    )
    assert post_event(url, update("Awaiting approval (appr-1): Send the quote"))[0] == 200
    mail.fail_next_body = 500
    card = approval_card("appr-1", requested_by=REQUESTER, approvers=[APPROVER])
    assert post_event(url, card) == (200, {"ref": CARD_REF})
    assert post_event(url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    assert "If none of the people who can approve is on this thread yet:" in request_email
    assert "Nobody on this thread" not in request_email
    assert mail.received_by(APPROVER) == [request_email]


@pytest.mark.parametrize(
    ("sender", "text", "headers", "thread_id", "has_reference"),
    [
        (ALLOWED_SENDER, "APPROVE", HUMAN_HEADERS, "thr-1", True),
        (COPIED, "REJECT\nPlease wait", HUMAN_HEADERS, "thr-1", True),
        ("Person <Copied@Example.COM>", "APPROVE", HUMAN_HEADERS, "thr-1", True),
        (ALLOWED_SENDER, "APPROVE", {**HUMAN_HEADERS, "Auto-Submitted": "no"}, "thr-1", True),
        (ALLOWED_SENDER, "Let me check", HUMAN_HEADERS, "thr-1", True),
        (ALLOWED_SENDER, None, HUMAN_HEADERS, "thr-1", True),
        (ALLOWED_SENDER, "APPROVE", HUMAN_HEADERS, "thr-1", False),
        (ALLOWED_SENDER, "APPROVE", HUMAN_HEADERS, "thr-other", True),
    ],
)
def test_every_new_approval_answer_shape_is_refused_before_resolution(
    mail: MailState,
    ingress: IngressState,
    approvals_adapter: MailAdapter,
    url: str,
    sender: str,
    text: str | None,
    headers: dict[str, str],
    thread_id: str,
    has_reference: bool,
) -> None:
    reference = _ask(mail, approvals_adapter, url)
    mail.add_inbound(
        "msg-answer",
        thread_id,
        sender=sender,
        text=text,
        headers=headers,
        full_text=f"APPROVE\nApproval reference: {reference}" if has_reference else "APPROVE",
    )
    approvals_adapter.poll_once()
    approvals_adapter.poll_once()
    assert ingress.resolves == []
    assert ingress.delivery_ids() == []
    assert mail.replies_to("msg-answer") == []
    assert mail.body_calls.get("msg-answer", 0) == 0
    assert approvals_adapter.state.delivery("msg-answer") == {"state": "rejected", "turn": None}
    stored = approvals_adapter.state.approval_ref_for("appr-1")
    assert stored is not None and stored["state"] == "live"
