"""Egress: the neutral reply wire, and one threaded AgentMail reply per completion.

Three groups. The ported spike cases are the regression floor. The reply-target
cases pin that the reply goes to `target.reply_ref` and never to whatever message
the conversation record happens to hold, which is the race a second message in a
thread produces. The delivery-failure cases pin that a provider failure becomes
an egress failure the platform can retry, rather than a 200 that destroys the
worker's durable completion record and loses the email.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import pytest
from _support import (
    EGRESS_SECRET,
    INBOX,
    IngressState,
    MailState,
    adapter_env,
    completed,
    free_port,
    post_event,
    reply_post,
    spawn_adapter,
    stop,
    turn_status,
    update,
    wait_for_healthz,
    wait_until,
)
from curie_mail_adapter.adapter import EVENT_MARKER, MailAdapter
from curie_mail_adapter.agentmail import AgentMailClient
from curie_mail_adapter.config import MailAdapterConfig


def seed(mail: MailState, adapter: MailAdapter, message_id: str = "msg-1", **kwargs: Any) -> None:
    """One inbound message, admitted through the real poll path."""
    mail.add_inbound(message_id, kwargs.pop("thread_id", "thr-1"), **kwargs)
    adapter.poll_once()


# --- ported: the platform is authenticated before any side effect -------------


@pytest.mark.parametrize("secret", [None, "", "wrong-secret"])
def test_egress_rejects_a_bad_or_missing_secret_before_any_side_effect(
    mail: MailState, adapter: MailAdapter, egress_url: str, secret: str | None
) -> None:
    """Anyone who can reach the Service could otherwise forge a completion.

    The refused `reply.update` must not have landed either, which is asserted
    through the wire rather than off a field: a later real completion falls back
    to the empty-reply text instead of echoing the rejected update.
    """
    seed(mail, adapter)

    assert post_event(egress_url, update("streamed reply"), secret=secret)[0] == 401
    assert post_event(egress_url, completed("ev-1"), secret=secret)[0] == 401
    assert mail.replies == []

    post_event(egress_url, completed("ev-1"))

    ((_in_reply_to, text),) = mail.replies
    assert "streamed reply" not in text


def test_egress_accepts_the_configured_secret_and_acks_without_a_ref(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    seed(mail, adapter)

    status, body = post_event(egress_url, completed("ev-1"), secret=EGRESS_SECRET)

    assert status == 200
    assert body.get("ref") is None  # email mints no editable handle
    assert len(mail.replies) == 1


def test_reply_text_is_the_latest_update_and_carries_the_event_marker(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    seed(mail, adapter)

    post_event(egress_url, update("partial"))
    post_event(egress_url, update("the full answer"))
    post_event(egress_url, completed("ev-42"))

    ((in_reply_to, text),) = mail.replies
    assert in_reply_to == "msg-1"
    assert text.startswith("the full answer")
    assert "partial\n" not in text
    assert f"{EVENT_MARKER} ev-42" in text


def test_turn_status_is_ignored(mail: MailState, adapter: MailAdapter, egress_url: str) -> None:
    seed(mail, adapter)

    status, _ = post_event(egress_url, turn_status())

    assert status == 200
    assert mail.replies == []


def test_a_repeated_event_id_produces_exactly_one_email(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """Completion delivery is at-least-once, so the fast path dedupes on event_id."""
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))

    post_event(egress_url, completed("ev-1"))
    post_event(egress_url, completed("ev-1"))

    assert len(mail.replies) == 1


def test_a_restart_does_not_double_send_because_the_thread_carries_the_marker(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """The durable half of the dedupe: the marker in the thread survives the process."""
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    post_event(egress_url, completed("ev-1"))
    assert len(mail.replies) == 1

    adapter.replied_event_ids.clear()  # a pod restart, or a bounded-cache eviction

    post_event(egress_url, completed("ev-1"))

    assert len(mail.replies) == 1
    assert mail.thread_calls >= 1  # the durable check actually listed the thread


def test_after_a_restart_an_unmarked_event_id_still_sends(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    post_event(egress_url, completed("ev-1"))

    adapter.replied_event_ids.clear()

    post_event(egress_url, completed("ev-2"))

    assert len(mail.replies) == 2
    assert f"{EVENT_MARKER} ev-2" in mail.replies[1][1]


# --- ported: ADAPTER_INGRESS_ENABLED gates the poller, never the server -------
#
# Driven through the real `python -m curie_mail_adapter` entry point, because the
# flag's whole job is to decide what the process starts.


def test_ingress_disabled_serves_egress_and_never_polls(
    mail: MailState, ingress: IngressState
) -> None:
    port = free_port()
    proc = spawn_adapter(
        adapter_env(
            agentmail_base_url=mail.base_url,
            api_url=ingress.url,
            port=port,
            ingress_enabled="false",
        )
    )
    try:
        wait_for_healthz(port, proc)
        mail.add_inbound("msg-9", "thr-9")
        time.sleep(0.5)  # ten poll intervals

        status, _ = post_event(f"http://127.0.0.1:{port}/", turn_status())

        assert status == 200
        assert mail.list_calls == 0
        assert ingress.attempts == 0
    finally:
        stop(proc)


def test_ingress_enabled_polls_and_posts_new_mail(mail: MailState, ingress: IngressState) -> None:
    port = free_port()
    proc = spawn_adapter(
        adapter_env(agentmail_base_url=mail.base_url, api_url=ingress.url, port=port)
    )
    try:
        wait_for_healthz(port, proc)
        assert wait_until(lambda: mail.list_calls > 0), "the poller never primed"

        # Our own outbound reply carries the `sent` label and must never be ingested.
        mail.add_inbound("msg-sent", "thr-live", sender=INBOX, labels=["sent"])
        mail.add_inbound("msg-live", "thr-live")

        assert wait_until(lambda: bool(ingress.requests)), "new mail never reached ingress"
        time.sleep(0.3)  # a second pass must not re-post it

        assert ingress.delivery_ids() == ["msg-live"]
    finally:
        stop(proc)


# --- the reply target is the event's reply_ref, never the conversation record --


def test_interleaved_turns_each_reply_to_their_own_message(
    mail: MailState, ingress: IngressState, adapter: MailAdapter, egress_url: str
) -> None:
    """Two messages land in one thread before either turn completes.

    Reading the target off the conversation record instead of the event sends both
    answers to msg-2, because the record is overwritten by every inbound message.
    Every other egress case uses one message per thread, so this is the only test
    that can see that mutation.
    """
    mail.add_inbound("msg-1", "thr-1", subject="First", text="one")
    mail.add_inbound("msg-2", "thr-1", subject="Follow up", text="and this?")
    adapter.poll_once()
    assert ingress.delivery_ids() == ["msg-1", "msg-2"]

    post_event(egress_url, update("answer one"))
    post_event(egress_url, completed("ev-1", conversation_id="thr-1", reply_ref="msg-1"))
    post_event(egress_url, update("answer two"))
    post_event(egress_url, completed("ev-2", conversation_id="thr-1", reply_ref="msg-2"))

    assert [mid for mid, _text in mail.replies] == ["msg-1", "msg-2"]
    assert mail.replies[1][1].startswith("answer two")

    # The late duplicate of turn one must not re-send turn one's stale text.
    post_event(egress_url, completed("ev-1", conversation_id="thr-1", reply_ref="msg-1"))

    assert len(mail.replies) == 2


def test_a_second_message_does_not_erase_an_answer_already_emitted(
    mail: MailState, ingress: IngressState, adapter: MailAdapter, egress_url: str
) -> None:
    """The interleaving above admits both messages before either turn speaks.

    Reverse that order, which is the ordinary case (a correspondent replies again
    while the agent is still working), and `handle_inbound` resetting the shared
    record to {"text": None} throws away turn one's finished answer. Turn one
    then sends the empty fallback, and the real answer is never delivered: the
    worker saw a 200. Only the record's TEXT is at stake here; the reply target
    still comes off the event.
    """
    mail.add_inbound("msg-1", "thr-1", subject="First", text="one")
    adapter.poll_once()
    post_event(egress_url, update("answer one"))

    mail.add_inbound("msg-2", "thr-1", subject="Follow up", text="and this?")
    adapter.poll_once()
    assert ingress.delivery_ids() == ["msg-1", "msg-2"]

    post_event(egress_url, completed("ev-1", conversation_id="thr-1", reply_ref="msg-1"))

    ((in_reply_to, text),) = mail.replies
    assert in_reply_to == "msg-1"
    assert text.startswith("answer one")


def test_a_completion_with_a_null_reply_ref_sends_nothing(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """There is no fallback to a stored message id, because that fallback is the bug."""
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))

    status, _ = post_event(egress_url, completed("ev-1", reply_ref=None))

    assert status == 200
    assert mail.replies == []


def test_a_completion_for_an_unknown_conversation_is_a_retryable_failure(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """Nothing is sent, and the platform is told so rather than thanked for it.

    `reply_ref` names a real, sendable message here, so only the record gate can
    be what stops the send: the inbound checks keep their transitive reach into
    egress. What must NOT happen is the 200. The worker clears its durable
    completion record on any 2xx (`kernel.py` clear_completion), so a 200 that
    sent no email is a permanent loss with no retry and no dead letter, and the
    adapter cannot tell a forged conversation_id from one a restart erased. 502
    is the code this suite already uses for a send that did not happen: the
    platform retries it and eventually dead-letters it, which is visible.
    """
    seed(mail, adapter)

    status, _ = post_event(
        egress_url, completed("ev-forged", conversation_id="thr-unknown", reply_ref="msg-1")
    )

    assert status == 502
    assert mail.replies == []


def test_a_restart_that_erases_the_conversation_record_is_not_acked_as_delivered(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """The restart a credential rotation mid-turn actually produces.

    `conversations` is process-local, so the pod comes back with the completion
    still undelivered and no record for it. The marker-in-thread test above
    clears only `replied_event_ids`, which is the half a restart does not make
    dangerous; this clears the half that does.
    """
    seed(mail, adapter)
    post_event(egress_url, update("the answer"))
    adapter.conversations.clear()  # the pod restarted between the update and the completion

    status, _ = post_event(egress_url, completed("ev-1"))

    assert status == 502
    assert mail.replies == []


def test_a_delivered_reply_is_acked_after_a_restart_that_erased_both_maps(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """The durable marker outranks the missing record, and it has to be consulted first.

    The case above is a restart between the update and the send: nothing went
    out, the thread proves nothing, and 502 is right. This is the restart AFTER
    the send, before the worker got its ack. The redelivery finds an empty
    process-local `conversations`, and answering 502 off that alone refuses an
    already-delivered completion on every retry until the platform dead-letters
    it. The thread itself carries `X-Curie-Event: ev-1`, which is the whole point
    of the durable half of the dedupe: it survives the process, so it is the
    answer, and 200 lets the worker clear a record whose email really was sent.
    """
    seed(mail, adapter)
    post_event(egress_url, update("the answer"))
    assert post_event(egress_url, completed("ev-1"))[0] == 200
    assert len(mail.replies) == 1
    assert f"{EVENT_MARKER} ev-1" in mail.replies[0][1]

    adapter.replied_event_ids.clear()  # the pod restarted after the provider accepted it
    adapter.conversations.clear()  # ... and the conversation record went with it

    status, _ = post_event(egress_url, completed("ev-1"))

    assert status == 200
    assert len(mail.replies) == 1


def test_a_later_turn_does_not_email_the_previous_turns_answer_again(
    mail: MailState, ingress: IngressState, adapter: MailAdapter, egress_url: str
) -> None:
    """Preserving the reply text across an inbound message must not preserve it forever.

    The case above is why the record is no longer reset on inbound: turn one's
    answer has to survive a second message arriving. But nothing clears the text
    once turn one has actually been emailed, so the record still holds "answer
    one" when turn two starts. A turn that emits only a `reply.post`, an approval
    card being the ordinary one, appends to it, and the correspondent is emailed
    turn one's answer a second time above the card. Only a second turn driven
    through `reply.post` to completion can see it; every other case here either
    ends at turn one or overwrites the text with a `reply.update`.
    """
    mail.add_inbound("msg-1", "thr-1", subject="First", text="one")
    adapter.poll_once()
    post_event(egress_url, update("answer one"))
    post_event(egress_url, completed("ev-1", conversation_id="thr-1", reply_ref="msg-1"))
    assert mail.replies[0][1].startswith("answer one")

    mail.add_inbound("msg-2", "thr-1", subject="Follow up", text="ship it?")
    adapter.poll_once()
    assert ingress.delivery_ids() == ["msg-1", "msg-2"]

    post_event(egress_url, reply_post("Approve this deploy?"))
    post_event(egress_url, completed("ev-2", conversation_id="thr-1", reply_ref="msg-2"))

    assert len(mail.replies) == 2
    in_reply_to, second = mail.replies[1]
    assert in_reply_to == "msg-2"
    assert "Approve this deploy?" in second
    assert "answer one" not in second


# --- a provider failure is an egress failure ----------------------------------


@pytest.mark.parametrize("provider_status", [429, 500, 0])
def test_a_provider_failure_becomes_an_egress_failure(
    mail: MailState, adapter: MailAdapter, egress_url: str, provider_status: int
) -> None:
    """Status 0 is the transport failure; the spike treated all three as success.

    Answering 200 here loses the email permanently AND suppresses every redelivery
    the platform would have made, because a 2xx is the worker's signal to clear the
    durable completion record.
    """
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    mail.fail_next_reply = provider_status

    status, _ = post_event(egress_url, completed("ev-1"))

    assert status == 502
    assert mail.replies == []


def test_a_retry_after_a_provider_failure_delivers_exactly_once(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """The fix has to recover the email, not merely report the failure."""
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    mail.fail_next_reply = 500
    assert post_event(egress_url, completed("ev-1"))[0] == 502

    status, _ = post_event(egress_url, completed("ev-1"))

    assert status == 200
    assert len(mail.replies) == 1
    assert mail.replies[0][1].startswith("answer one")


def test_a_concurrent_duplicate_gets_a_503_and_does_not_double_send(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """A duplicate whose outcome is unknown must be answered as a failure.

    Answering 200 makes the worker clear the durable completion record
    (`kernel.py` clear_completion, on any 2xx) while the first attempt is still in
    flight, so if that attempt then fails the email is gone with no retry and no
    dead letter. 503 says "not yet, come back", and is distinguishable in the
    worker's log from the 502 that means the provider rejected the send.
    """
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    mail.hold_replies()
    results: dict[str, tuple[int, Any]] = {}

    def first() -> None:
        results["first"] = post_event(egress_url, completed("ev-1"))

    sender = threading.Thread(target=first, daemon=True)
    sender.start()
    assert mail.reply_entered.wait(20), "the first send never reached the provider"

    results["second"] = post_event(egress_url, completed("ev-1"))

    mail.release_replies()
    sender.join(20)
    assert results["second"][0] == 503
    assert results["first"][0] == 200
    assert len(mail.replies) == 1


def test_the_503d_duplicate_converges_once_the_first_attempt_settles(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """503 is "come back later", not a permanent refusal, so the retry terminates."""
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    mail.hold_replies()
    first_status: list[int] = []
    sender = threading.Thread(
        target=lambda: first_status.append(post_event(egress_url, completed("ev-1"))[0]),
        daemon=True,
    )
    sender.start()
    assert mail.reply_entered.wait(20)
    assert post_event(egress_url, completed("ev-1"))[0] == 503
    mail.release_replies()
    sender.join(20)
    assert first_status == [200]

    status, _ = post_event(egress_url, completed("ev-1"))

    assert status == 200
    assert len(mail.replies) == 1


@pytest.mark.parametrize("thread_status", [500, 0])
def test_an_unreadable_thread_does_not_send_a_duplicate_email(
    mail: MailState, adapter: MailAdapter, egress_url: str, thread_status: int
) -> None:
    """The durable half of the dedupe currently fails open, which sends twice.

    `thread_carries` reports "not present" when the thread could not be read at
    all, so once the fast path has been lost to a restart or an eviction, a
    redelivered completion is sent to the correspondent a second time. "Could
    not check" is not "not present": 502 asks the platform to come back, which
    is the only answer that neither duplicates nor loses, and the retry then
    finds the marker and settles.
    """
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    post_event(egress_url, completed("ev-1"))
    assert len(mail.replies) == 1

    adapter.replied_event_ids.clear()  # a pod restart, or a bounded-cache eviction
    mail.fail_next_thread = thread_status

    status, _ = post_event(egress_url, completed("ev-1"))

    assert status == 502
    assert len(mail.replies) == 1

    assert post_event(egress_url, completed("ev-1"))[0] == 200
    assert len(mail.replies) == 1


def test_a_403_from_the_provider_is_a_delivery_failure(
    mail: MailState, adapter: MailAdapter, egress_url: str
) -> None:
    """The spike logged "rejected by the send allow list; continuing" and acked 200."""
    seed(mail, adapter)
    post_event(egress_url, update("answer one"))
    mail.fail_next_reply = 403

    status, _ = post_event(egress_url, completed("ev-1"))

    assert status == 502
    assert mail.replies == []


class _RaiseOnceClient(AgentMailClient):
    """The real client, with an unexpected exception injected into the first reply.

    A deliberate, narrow carve-out to this package's mocking rule: it is a real
    subclass of the client that wraps the EXTERNAL provider, the same seam the fake
    HTTP servers occupy, and nothing inside `curie_mail_adapter` is patched. The
    fault has to be injected here because the behavior under test is the handling
    of an UNEXPECTED exception, and anything provokable through the provider's HTTP
    boundary is by definition an expected one the client turns into a failure
    result. It is the only such double in the suite and it is not a precedent.
    """

    def __init__(self, config: MailAdapterConfig) -> None:
        super().__init__(config)
        self.calls = 0

    def reply(self, message_id: str, text: str) -> tuple[int, Any]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("injected")
        return super().reply(message_id, text)


def test_an_unexpected_exception_does_not_poison_the_event_id(
    mail: MailState,
    make_config: Callable[..., MailAdapterConfig],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    """This is the mutation proof for the `try/finally` around the send.

    `in_flight` is added to before the outbound call and removed after it. Delete
    the `finally` and the raised exception leaks the event_id forever: the
    redelivery below finds it in `in_flight`, takes the concurrent-duplicate
    branch, sends nothing and answers 503, so both assertions below fail and
    nothing else in the suite changes. Cases 4 to 6 cannot see that mutation,
    because transport failures come back from the client as ordinary failure
    results rather than raised exceptions.
    """
    config = make_config()
    adapter = MailAdapter(config, client=_RaiseOnceClient(config))
    url = serve_egress(adapter) + "/"
    mail.add_inbound("msg-1", "thr-1")
    adapter.poll_once()
    post_event(url, update("answer one"))

    assert post_event(url, completed("ev-1"))[0] == 500

    status, _ = post_event(url, completed("ev-1"))

    assert status == 200
    assert len(mail.replies) == 1
