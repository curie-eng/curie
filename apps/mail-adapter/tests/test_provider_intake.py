"""The intake path and the approval reply path, through a provider that verifies senders.

ADR 0228 rebuilds the intake path once, in the neutral adapter, behind a gate
only a verifying provider can open. These tests drive it end to end: the real
adapter, its own egress server, the fake AgentMail HTTP server behind
`VerifyingTestProvider`, and the fake platform. Nothing inside
`curie_mail_adapter` is patched.

The gate (decision 3): before any fetch, a provider that does not verify senders
refuses everything, and the listed From must be one mailbox the sender filter
admits; after the fetch, that one From must pass the filter, the verdict must be
``verified``, and the verdict's domain must equal the From domain exactly.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator

import pytest
from _support import (
    ALLOWED_SENDER,
    INBOX,
    STRANGER,
    IngressState,
    MailState,
    approval_card,
    completed,
    post_event,
    settled_card,
    update,
)
from _verifying_provider import VerifyingTestProvider, failed, verified
from curie_mail_adapter.adapter import (
    APPROVAL_CARD_REF_PREFIX,
    APPROVAL_REF_PATTERN,
    MailAdapter,
)
from curie_mail_adapter.config import MailAdapterConfig

PRINCIPAL = "adp.test-payload.test-signature"
APPROVER = "approver@example.com"
HUMAN_HEADERS = {"From": ALLOWED_SENDER, "Message-ID": "<human@example.com>"}
APPROVER_HEADERS = {"From": APPROVER, "Message-ID": "<approver@example.com>"}
CARD_REF = f"{APPROVAL_CARD_REF_PREFIX}appr-1"


@pytest.fixture
def make_verifying(
    make_config: Callable[..., MailAdapterConfig],
) -> Iterator[Callable[..., MailAdapter]]:
    built: list[MailAdapter] = []

    def _make(*, sent_visible_within: float = 0.0, **overrides: object) -> MailAdapter:
        config = make_config(**overrides)
        provider = VerifyingTestProvider(config, sent_visible_within=sent_visible_within)
        instance = MailAdapter(config, provider=provider)
        built.append(instance)
        return instance

    yield _make
    for instance in built:
        instance.shutdown.set()


@pytest.fixture
def verifying(make_verifying: Callable[..., MailAdapter]) -> MailAdapter:
    return make_verifying()


def _inbound(
    mail: MailState,
    message_id: str,
    thread_id: str = "thr-1",
    *,
    sender: str = ALLOWED_SENDER,
    verdict: dict[str, str] | None = None,
    **kwargs: object,
) -> None:
    mail.add_inbound(message_id, thread_id, sender=sender, **kwargs)  # type: ignore[arg-type]
    if verdict is not None:
        mail.verdicts[message_id] = verdict


# --- the turn ------------------------------------------------------------------


def test_a_verified_allowed_sender_becomes_a_turn_and_gets_its_reply(
    mail: MailState,
    ingress: IngressState,
    verifying: MailAdapter,
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    _inbound(
        mail,
        "msg-1",
        sender=f"Human <{ALLOWED_SENDER}>",
        subject="Quote",
        text="Please send the quote",
        verdict=verified("example.com"),
    )

    assert verifying.poll_once() == 200

    ((_headers, turn),) = ingress.requests
    assert turn == {
        "kind": "email",
        "address": INBOX,
        "delivery_id": "msg-1",
        "conversation_id": "thr-1",
        "author": ALLOWED_SENDER,
        "text": "Quote\n\nPlease send the quote",
        "reply_ref": "msg-1",
    }
    assert verifying.state.delivery("msg-1")["state"] == "accepted"  # type: ignore[index]

    url = serve_egress(verifying) + "/"
    assert post_event(url, update("Here it is"))[0] == 200
    assert post_event(url, completed("ev-1"))[0] == 200
    (reply,) = mail.replies_to("msg-1")
    assert reply.startswith("Here it is")


def test_the_turn_text_falls_back_from_new_text_to_full_text_to_html(
    mail: MailState, ingress: IngressState, verifying: MailAdapter
) -> None:
    _inbound(
        mail,
        "msg-full",
        "thr-a",
        text=None,
        full_text="the whole body",
        verdict=verified("example.com"),
    )
    _inbound(mail, "msg-html", "thr-b", text=None, verdict=verified("example.com"))
    mail.bodies["msg-html"]["html"] = "<p>only html</p>"

    verifying.poll_once()

    texts = {body["delivery_id"]: body["text"] for _headers, body in ingress.requests}
    assert texts == {"msg-full": "Hello\n\nthe whole body", "msg-html": "Hello\n\n<p>only html</p>"}


@pytest.mark.parametrize(
    ("verdict", "sender"),
    [
        (None, ALLOWED_SENDER),
        (failed(), ALLOWED_SENDER),
        (verified("evil.example"), ALLOWED_SENDER),
        (verified("mail.example.com"), ALLOWED_SENDER),
        (verified("com"), ALLOWED_SENDER),
    ],
    ids=["unverifiable", "failed", "other_domain", "subdomain", "parent_domain"],
)
def test_a_message_without_an_aligned_verified_verdict_is_refused_after_the_fetch(
    mail: MailState,
    ingress: IngressState,
    verifying: MailAdapter,
    verdict: dict[str, str] | None,
    sender: str,
) -> None:
    """Forged sender-written authentication headers count for nothing."""
    _inbound(
        mail,
        "msg-1",
        sender=sender,
        verdict=verdict,
        headers={
            "From": sender,
            "Authentication-Results": "mx.example.com; dmarc=pass header.from=example.com",
        },
    )

    verifying.poll_once()

    assert ingress.requests == []
    assert mail.body_calls == {"msg-1": 1}
    assert verifying.state.delivery("msg-1") == {"state": "rejected", "turn": None}
    assert verifying.state.live_reply_refs("thr-1") == []


def test_a_sender_the_filter_refuses_is_rejected_before_any_fetch(
    mail: MailState, ingress: IngressState, verifying: MailAdapter
) -> None:
    _inbound(mail, "msg-1", sender=STRANGER, verdict=verified("evil.example"))

    verifying.poll_once()

    assert mail.body_calls == {}
    assert ingress.requests == []
    assert verifying.state.delivery("msg-1") == {"state": "rejected", "turn": None}


def test_two_from_mailboxes_are_refused_before_and_after_the_fetch(
    mail: MailState, ingress: IngressState, verifying: MailAdapter
) -> None:
    _inbound(
        mail,
        "msg-listed",
        "thr-a",
        sender=f"{ALLOWED_SENDER}, {STRANGER}",
        verdict=verified("example.com"),
    )
    # Listed with one From, read with two: the read is what the gate decides on.
    _inbound(mail, "msg-read", "thr-b", verdict=verified("example.com"))
    mail.bodies["msg-read"]["from"] = f"{ALLOWED_SENDER}, {STRANGER}"

    verifying.poll_once()

    assert ingress.requests == []
    assert mail.body_calls == {"msg-read": 1}
    for message_id in ("msg-listed", "msg-read"):
        assert verifying.state.delivery(message_id) == {"state": "rejected", "turn": None}


def test_a_failed_body_fetch_stays_pending_and_the_next_pass_delivers_it(
    mail: MailState, ingress: IngressState, verifying: MailAdapter
) -> None:
    _inbound(mail, "msg-1", verdict=verified("example.com"))
    mail.fail_next_body = 500

    verifying.poll_once()
    assert ingress.requests == []
    assert verifying.state.delivery("msg-1")["state"] == "body_pending"  # type: ignore[index]

    verifying.poll_once()

    assert ingress.delivery_ids() == ["msg-1"]
    assert verifying.state.delivery("msg-1")["state"] == "accepted"  # type: ignore[index]


def test_a_turn_the_platform_could_not_take_is_posted_again_from_the_pending_retry(
    mail: MailState, ingress: IngressState, make_verifying: Callable[..., MailAdapter]
) -> None:
    adapter = make_verifying(ingress_attempts=1)
    _inbound(mail, "msg-1", verdict=verified("example.com"))
    ingress.responses.append((503, {"detail": "busy"}, {}))

    adapter.poll_once()
    assert adapter.state.delivery("msg-1")["state"] == "ingress_pending"  # type: ignore[index]

    adapter.poll_once()

    assert ingress.delivery_ids() == ["msg-1", "msg-1"]
    assert adapter.state.delivery("msg-1")["state"] == "accepted"  # type: ignore[index]


def test_a_caller_list_refusal_settles_the_message_and_closes_its_reply_slot(
    mail: MailState, ingress: IngressState, verifying: MailAdapter
) -> None:
    _inbound(mail, "msg-1", verdict=verified("example.com"))
    ingress.responses.append((403, {"detail": "caller_not_allowed"}, {}))

    verifying.poll_once()
    verifying.poll_once()

    assert ingress.delivery_ids() == ["msg-1"]
    assert verifying.state.delivery("msg-1") == {"state": "rejected", "turn": None}
    assert verifying.state.live_reply_refs("thr-1") == []


def test_mail_already_in_the_inbox_at_first_boot_never_becomes_a_turn(
    mail: MailState, ingress: IngressState, verifying: MailAdapter
) -> None:
    _inbound(mail, "msg-old", verdict=verified("example.com"))
    _inbound(mail, "msg-stranger", "thr-2", sender=STRANGER)

    verifying.prime()
    verifying.poll_once()

    assert ingress.requests == []
    assert mail.body_calls == {}
    assert verifying.state.delivery("msg-old") == {"state": "primed", "turn": None}
    assert verifying.state.delivery("msg-stranger") == {"state": "rejected", "turn": None}


# --- approvals by email, end to end ----------------------------------------------


def _ask(mail: MailState, adapter: MailAdapter, url: str) -> str:
    """A verified asking turn, its approval card, and the request email it sends."""
    _inbound(
        mail,
        "msg-1",
        text="Please send the quote",
        headers=HUMAN_HEADERS,
        verdict=verified("example.com"),
    )
    adapter.poll_once()
    assert post_event(url, update("Awaiting approval (appr-1): Send the quote"))[0] == 200
    status, ack = post_event(url, approval_card("appr-1", approvers=[APPROVER]))
    assert (status, ack) == (200, {"ref": CARD_REF})
    assert post_event(url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    (reference,) = APPROVAL_REF_PATTERN.findall(request_email)
    return str(reference)


@pytest.mark.parametrize(("word", "decision"), [("APPROVE", "approved"), ("REJECT", "rejected")])
def test_a_verified_approver_answers_by_reply_and_the_outcome_follows(
    mail: MailState,
    ingress: IngressState,
    make_verifying: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
    word: str,
    decision: str,
) -> None:
    adapter = make_verifying(
        adapter_principal=PRINCIPAL, allowed_senders=(ALLOWED_SENDER, APPROVER)
    )
    url = serve_egress(adapter) + "/"
    reference = _ask(mail, adapter, url)

    _inbound(
        mail,
        "msg-answer",
        sender=APPROVER,
        text=f"{word}\nChecked the numbers",
        full_text=f"{word}\nChecked the numbers\n\n> Approval reference: {reference}",
        headers=APPROVER_HEADERS,
        cc=[ALLOWED_SENDER],
        verdict=verified("example.com"),
    )
    adapter.poll_once()

    ((path, headers, body),) = ingress.resolves
    assert path == "/approvals/appr-1/resolve"
    assert body == {"decision": decision, "note": "Checked the numbers"}
    assert headers["X-Curie-Approval-Actor"] == APPROVER
    assert headers["X-Curie-Adapter-Principal"] == PRINCIPAL
    # An answer is never a turn.
    assert ingress.delivery_ids() == ["msg-1"]
    assert adapter.state.delivery("msg-answer") == {"state": "answered", "turn": None}

    assert post_event(url, settled_card(CARD_REF, decision=decision, resolver=APPROVER))[0] == 200
    (follow_up,) = mail.replies_to("msg-answer")
    assert f"This request was {decision} by {APPROVER}." in follow_up


def test_no_answer_is_carried_without_a_verified_verdict(
    mail: MailState,
    ingress: IngressState,
    make_verifying: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    adapter = make_verifying(
        adapter_principal=PRINCIPAL, allowed_senders=(ALLOWED_SENDER, APPROVER)
    )
    url = serve_egress(adapter) + "/"
    reference = _ask(mail, adapter, url)

    _inbound(
        mail,
        "msg-answer",
        sender=APPROVER,
        text="APPROVE",
        full_text=f"APPROVE\n\n> Approval reference: {reference}",
        headers={
            **APPROVER_HEADERS,
            "Authentication-Results": "mx.example.com; dmarc=pass header.from=example.com",
        },
        verdict=failed(),
    )
    adapter.poll_once()

    assert ingress.resolves == []
    assert mail.replies_to("msg-answer") == []
    assert adapter.state.delivery("msg-answer") == {"state": "rejected", "turn": None}
    ref = adapter.state.approval_ref_for("appr-1")
    assert ref is not None and ref["state"] == "live"


def test_a_sender_the_filter_refuses_never_reaches_approval_logic(
    mail: MailState,
    ingress: IngressState,
    make_verifying: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    adapter = make_verifying(adapter_principal=PRINCIPAL, allowed_senders=(ALLOWED_SENDER,))
    url = serve_egress(adapter) + "/"
    reference = _ask(mail, adapter, url)

    _inbound(
        mail,
        "msg-answer",
        sender=STRANGER,
        text="APPROVE",
        full_text=f"APPROVE\n\n> Approval reference: {reference}",
        verdict=verified("evil.example"),
    )
    adapter.poll_once()

    assert ingress.resolves == []
    assert "msg-answer" not in mail.body_calls
    assert mail.replies_to("msg-answer") == []


# --- a provider whose sent copy becomes visible only within a bound --------------


def test_a_retry_inside_the_visibility_bound_waits_instead_of_sending_twice(
    mail: MailState,
    ingress: IngressState,
    make_verifying: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    """Decision 2: the send attempt is recorded before the send, so a lost answer
    inside the bound retries later instead of mailing a duplicate, and after the
    bound "not in the thread" means the send did not land."""
    adapter = make_verifying(sent_visible_within=0.5)
    url = serve_egress(adapter) + "/"
    _inbound(mail, "msg-1", verdict=verified("example.com"))
    adapter.poll_once()
    post_event(url, update("answer"))

    # The send lands but its answer is lost, and the fake files nothing in the
    # thread witness for this test: the sent copy is not visible yet.
    mail.fail_next_reply = 0
    assert post_event(url, completed("ev-1"))[0] == 502
    assert mail.replies_to("msg-1") == []

    assert post_event(url, completed("ev-1"))[0] == 502
    assert mail.replies_to("msg-1") == []

    time.sleep(0.6)
    assert post_event(url, completed("ev-1"))[0] == 200
    assert len(mail.replies_to("msg-1")) == 1


def test_a_provider_with_read_after_write_visibility_sends_on_the_first_retry(
    mail: MailState,
    ingress: IngressState,
    verifying: MailAdapter,
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    url = serve_egress(verifying) + "/"
    _inbound(mail, "msg-1", verdict=verified("example.com"))
    verifying.poll_once()
    post_event(url, update("answer"))
    mail.fail_next_reply = 503

    assert post_event(url, completed("ev-1"))[0] == 502
    assert post_event(url, completed("ev-1"))[0] == 200
    assert len(mail.replies_to("msg-1")) == 1
