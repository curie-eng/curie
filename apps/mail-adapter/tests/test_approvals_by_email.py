"""Approvals answered by email (ADR-0177), through the adapter's real surfaces.

The worker's approval card arrives on the adapter's own egress server, the
request email leaves through the fake AgentMail, the requester's reply comes in
through the real poll path, and the answer leaves as a resolve call to the fake
platform. Nothing inside the adapter is patched.

Each rule the ADR sets for accepting a reply is pinned by a refusal: a copied
person, an auto-reply, a reply without headers, a decision only in the quote,
a spent reference, and a reply naming no reference. None of them resolves, and
none of them starts a turn.
"""

from __future__ import annotations

import re
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


def test_the_requester_answers_by_reply_and_the_thread_gets_one_follow_up(
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


def test_a_person_copied_on_the_thread_cannot_answer(
    mail: MailState, ingress: IngressState, approvals_adapter: MailAdapter, url: str
) -> None:
    reference = _ask(mail, approvals_adapter, url)

    _reply(mail, approvals_adapter, "msg-2", "APPROVE", reference=reference, sender=COPIED)

    assert ingress.resolves == []
    assert _notices(mail, "msg-2") == ["Only the person who asked can answer this approval."]
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
        (403, "Your answer could not be accepted for this approval."),
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
