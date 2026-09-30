"""Approvals answered by email (ADR-0177), through the adapter's real surfaces.

The worker's approval card arrives on the adapter's own egress server, the
request email leaves through the fake AgentMail, the requester's reply comes in
through the real poll path, and the answer leaves as a resolve call to the fake
platform. Nothing inside the adapter is patched.

Each rule the ADR sets for accepting a reply is pinned by a refusal: a sender
the inbound gate did not verify, a sender the mailbox does not admit, an
auto-reply, a reply without headers, a decision only in the quote, a spent
reference, and a reply naming no reference. None of them resolves, and none of
them starts a turn. Who may answer is the platform's decision (ADR 0183): the
adapter carries the verified sender's bare address, and the fake platform's
refusals pin what the adapter then tells the sender.
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
HUMAN_HEADERS = {"From": ALLOWED_SENDER, "Message-ID": "<reply@example.com>"}
CARD_REF = f"{APPROVAL_CARD_REF_PREFIX}appr-1"


@pytest.fixture
def approvals_adapter(make_adapter: Callable[..., MailAdapter]) -> Any:
    instance = make_adapter(
        adapter_principal=PRINCIPAL, allowed_senders=(ALLOWED_SENDER, COPIED)
    )
    yield instance
    instance.shutdown.set()


@pytest.fixture
def url(approvals_adapter: MailAdapter, serve_egress: Callable[[MailAdapter], str]) -> str:
    return serve_egress(approvals_adapter) + "/"


def _ask(mail: MailState, adapter: MailAdapter, url: str) -> str:
    """Run one turn to the approval pause and return the reference it mailed."""

    mail.add_inbound("msg-1", "thr-1", text="Please send the quote")
    adapter.poll_once()
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


# --- the whole loop ------------------------------------------------------------


def test_an_answer_by_reply_is_carried_and_the_thread_gets_one_follow_up(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    _reply(
        mail, approvals_adapter, "msg-2", "Approve.\nThe numbers look right.", reference=reference
    )

    # One resolve, carried by the adapter's credential with the sender as actor.
    (resolve,) = ingress.resolves
    path, headers, body = resolve
    assert path == "/approvals/appr-1/resolve"
    assert headers["X-Curie-Adapter-Principal"] == PRINCIPAL
    assert headers["X-Curie-Approval-Actor"] == ALLOWED_SENDER
    assert body == {"decision": "approved", "note": "The numbers look right."}
    # The answer never became a turn, and nobody was mailed yet.
    assert ingress.delivery_ids() == ["msg-1"]
    assert _notices(mail, "msg-2") == []

    # A second answer before the card settles is not carried: first answer wins.
    _reply(mail, approvals_adapter, "msg-2b", "REJECT", reference=reference)
    assert len(ingress.resolves) == 1
    assert _notices(mail, "msg-2b") == ["This approval has already been answered."]

    # The worker settles the card: one short follow-up in the thread.
    assert post_event(
        url, settled_card(CARD_REF, decision="approved", resolver=ALLOWED_SENDER, note="ok")
    )[0] == 200
    follow_up = mail.replies_to("msg-1")[-1]
    assert follow_up.startswith(f"This request was approved by {ALLOWED_SENDER}.")
    # A redelivered settle sends nothing more.
    assert post_event(url, settled_card(CARD_REF, decision="approved"))[0] == 200
    assert len(mail.replies_to("msg-1")) == 2

    # The resumed turn answers on the asking message.
    assert post_event(url, update("Sent the quote.", reply_ref="msg-1"))[0] == 200
    assert post_event(url, completed("ev-2"))[0] == 200
    assert mail.replies_to("msg-1")[-1].startswith("Sent the quote.")

    # The reference is spent: a replayed answer decides nothing.
    _reply(mail, approvals_adapter, "msg-3", "APPROVE", reference=reference)
    assert len(ingress.resolves) == 1
    assert _notices(mail, "msg-3") == ["This approval has already been answered."]
    assert ingress.delivery_ids() == ["msg-1"]


def test_expiry_sends_the_expired_follow_up_and_spends_the_reference(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    assert post_event(url, settled_card(CARD_REF, decision=None))[0] == 200
    assert mail.replies_to("msg-1")[-1] == "This approval expired before anyone answered it."

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference)
    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == ["This approval has already been answered."]


# --- each acceptance rule, refused ---------------------------------------------


def test_any_admitted_sender_is_carried_and_the_platform_decides(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """The adapter no longer keeps the answer to the person who asked: a listed
    approver is often someone else on the thread. It carries the sender, and a
    platform refusal is told plainly."""

    reference = _ask(mail, approvals_adapter, url)
    ingress.resolve_responses = [
        (403, {"detail": "you are not an approver: this approval's route is bound to "
               "an explicit list of approver email addresses"})
    ]

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference, sender=COPIED)

    (resolve,) = ingress.resolves
    assert resolve[1]["X-Curie-Approval-Actor"] == COPIED
    assert _notices(mail, "msg-2") == ["You are not an approver for this request."]
    assert ingress.delivery_ids() == ["msg-1"]


def test_the_actor_is_the_verified_bare_address_never_the_display_name(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    _reply(
        mail,
        approvals_adapter,
        "msg-2",
        "APPROVE",
        reference=reference,
        sender="Approver Person <Copied@Example.COM>",
    )

    (resolve,) = ingress.resolves
    assert resolve[1]["X-Curie-Approval-Actor"] == COPIED


@pytest.mark.parametrize("label", ["unauthenticated", "spam", "blocked"])
def test_a_listed_address_the_inbound_gate_did_not_verify_is_never_carried(
    mail: MailState,
    ingress: IngressState,
    approvals_adapter: MailAdapter,
    url: str,
    label: str,
) -> None:
    """ADR 0183 decision 2, step 1: the provider's SPF, DKIM and DMARC verdict
    comes first. A forged message from an address the route lists never reaches
    the approval logic, gets nothing back, and is never a turn. The fake serves
    the labeled message, as a provider whose default filtering widened would, so
    the adapter's own label gate is what refuses it."""

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
    assert ingress.delivery_ids() == ["msg-1"]


def test_a_sender_the_mailbox_does_not_admit_is_refused_before_any_approval_logic(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """ADR 0183 decision 2, step 2: the inbound allowlist comes before the
    reference, the reply rules and the approver list."""

    reference = _ask(mail, approvals_adapter, url)

    _reply(
        mail, approvals_adapter, "msg-2", "APPROVE", reference=reference,
        sender="stranger@example.net",
    )

    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == []
    assert ingress.delivery_ids() == ["msg-1"]


def test_a_platform_caller_refusal_gets_nothing_back(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """The binding's allowed_callers refused the sender (ADR 0175): as on a turn,
    a refused caller is told nothing, and the answer is not retried."""

    reference = _ask(mail, approvals_adapter, url)
    ingress.resolve_responses = [(403, {"detail": "caller_not_allowed"})]

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference, sender=COPIED)
    approvals_adapter.poll_once()

    assert len(ingress.resolves) == 1
    assert _notices(mail, "msg-2") == []
    assert ingress.delivery_ids() == ["msg-1"]


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
    assert ingress.delivery_ids() == ["msg-1"]


def test_auto_submitted_no_is_a_person(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """RFC 3834 section 5: "no" is the value a person's message carries."""

    reference = _ask(mail, approvals_adapter, url)
    _reply(
        mail,
        approvals_adapter,
        "msg-2",
        "REJECT",
        reference=reference,
        headers={**HUMAN_HEADERS, "Auto-Submitted": "no"},
    )
    assert [body["decision"] for _path, _headers, body in ingress.resolves] == ["rejected"]


def test_a_decision_only_in_the_quote_is_not_an_answer(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    _reply(
        mail,
        approvals_adapter,
        "msg-2",
        "Let me check with the team first.",
        reference=reference,
        quoted="APPROVE",
    )

    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == [APPROVAL_INSTRUCTIONS]
    assert ingress.delivery_ids() == ["msg-1"]


def test_a_decision_outside_the_extracted_new_text_is_not_an_answer(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """A forward, or any message the provider extracted no new text from, is
    read only by its full body, which cannot separate the sender's words from
    what they quoted (https://www.agentmail.to/docs/messages)."""

    reference = _ask(mail, approvals_adapter, url)
    mail.add_inbound(
        "msg-2",
        "thr-1",
        text=None,
        full_text=f"APPROVE\n---------- Forwarded message ----------\n{reference}",
        headers=HUMAN_HEADERS,
    )
    approvals_adapter.poll_once()
    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == [APPROVAL_INSTRUCTIONS]


def test_a_reply_without_new_text_is_not_an_answer(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)
    _reply(mail, approvals_adapter, "msg-2", None, reference=reference)
    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == [APPROVAL_INSTRUCTIONS]


def test_a_reply_in_a_pending_thread_naming_no_reference_gets_the_instructions(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    _ask(mail, approvals_adapter, url)

    mail.add_inbound("msg-2", "thr-1", text="APPROVE", full_text="APPROVE", headers=HUMAN_HEADERS)
    approvals_adapter.poll_once()

    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == [APPROVAL_INSTRUCTIONS]
    assert ingress.delivery_ids() == ["msg-1"]


def test_a_reference_from_another_thread_does_not_answer_this_one(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    # The same words in a thread with no approval of this adapter's: an
    # ordinary turn, and nothing is resolved.
    mail.add_inbound(
        "msg-9",
        "thr-2",
        text="APPROVE",
        full_text=f"APPROVE\n> Approval reference: {reference}",
        headers=HUMAN_HEADERS,
    )
    approvals_adapter.poll_once()

    assert ingress.resolves == []
    assert ingress.delivery_ids() == ["msg-1", "msg-9"]


# --- what the platform answers --------------------------------------------------


def test_a_platform_outage_keeps_the_answer_pending_and_a_later_pass_carries_it(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)
    ingress.resolve_responses = [(503, {"detail": "unavailable"})]

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference)
    assert len(ingress.resolves) == 1
    approvals_adapter.poll_once()

    assert [body["decision"] for _path, _headers, body in ingress.resolves] == [
        "approved",
        "approved",
    ]
    assert _notices(mail, "msg-2") == []
    assert ingress.delivery_ids() == ["msg-1"]


@pytest.mark.parametrize(
    ("status", "notice"),
    [
        (409, "This approval has already been answered."),
        (410, "This approval expired before it was answered."),
        (403, "You are not an approver for this request."),
        (422, "Your answer could not be accepted for this approval."),
    ],
)
def test_a_refused_answer_is_told_why(
    mail: MailState,
    ingress: IngressState,
    approvals_adapter: MailAdapter,
    url: str,
    status: int,
    notice: str,
) -> None:
    reference = _ask(mail, approvals_adapter, url)
    ingress.resolve_responses = [(status, {"detail": "refused"})]

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference)

    assert len(ingress.resolves) == 1
    assert _notices(mail, "msg-2") == [notice]


# --- without a credential nothing changes ----------------------------------------


def test_without_an_adapter_principal_a_card_is_plain_text_as_before(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    egress_url: str,
) -> None:
    mail.add_inbound("msg-1", "thr-1", text="Please send the quote")
    adapter.poll_once()
    status, ack = post_event(egress_url, approval_card("appr-1"))
    assert (status, ack) == (200, {"ref": None})
    assert post_event(egress_url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    assert APPROVAL_INSTRUCTIONS not in request_email
    assert not re.search(APPROVAL_REF_PATTERN, request_email)

    mail.add_inbound("msg-2", "thr-1", text="APPROVE", full_text="APPROVE", headers=HUMAN_HEADERS)
    adapter.poll_once()
    assert ingress.resolves == []
    assert ingress.delivery_ids() == ["msg-1", "msg-2"]


def test_a_redelivered_card_keeps_its_one_reference(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    mail.add_inbound("msg-1", "thr-1", text="Please send the quote")
    approvals_adapter.poll_once()
    assert post_event(url, approval_card("appr-1"))[0] == 200
    assert post_event(url, approval_card("appr-1"))[0] == 200
    assert post_event(url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    assert len(set(APPROVAL_REF_PATTERN.findall(request_email))) == 1


# --- settlement is sent once, and never skipped ---------------------------------


def test_a_lost_answer_response_still_gets_its_follow_up_and_the_resumed_reply(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """The platform took the first answer but its response was lost: the retry
    reads 409. The card's settlement must still send the follow-up and reopen
    the asking reply, or the resumed turn has nowhere to answer."""

    reference = _ask(mail, approvals_adapter, url)
    ingress.resolve_responses = [(409, {"detail": f"already resolved by {ALLOWED_SENDER}"})]
    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference)

    assert post_event(url, settled_card(CARD_REF, decision="approved"))[0] == 200
    assert mail.replies_to("msg-1")[-1].startswith("This request was approved")
    assert post_event(url, update("Sent the quote.", reply_ref="msg-1"))[0] == 200
    assert post_event(url, completed("ev-2"))[0] == 200
    assert mail.replies_to("msg-1")[-1].startswith("Sent the quote.")


def test_a_resumed_turn_that_posts_after_the_card_answers_the_asking_message(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    """ADR-0179 decision 3: the resumed turn drops the replayed placeholder, so
    its first delivery names no ref. The ack names the asking message, the
    worker keeps the turn there, and the answer is mailed in the same thread."""

    reference = _ask(mail, approvals_adapter, url)
    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference)
    assert post_event(url, settled_card(CARD_REF, decision="approved"))[0] == 200

    status, ack = post_event(url, update("Sent the", reply_ref=None))
    assert (status, ack) == (200, {"ref": "msg-1"})
    assert post_event(url, update("Sent the quote.", reply_ref=ack["ref"]))[0] == 200
    assert post_event(url, completed("ev-2", reply_ref=ack["ref"]))[0] == 200
    assert mail.replies_to("msg-1")[-1].startswith("Sent the quote.")


def test_a_ref_less_post_is_acked_with_the_reply_owner_it_landed_on(
    mail: MailState, approvals_adapter: MailAdapter, url: str
) -> None:
    mail.add_inbound("msg-1", "thr-1", text="Please send the quote")
    approvals_adapter.poll_once()
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
