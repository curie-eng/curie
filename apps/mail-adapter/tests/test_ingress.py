"""Ingress: a polled AgentMail message becomes one POST /channels/turns.

The four ported spike cases (the regression floor) plus the two that pin the
`seen` bound, which is the one map an unauthenticated stranger can grow: the
insert happens before the allow-list decision, so no inbound gate mitigates an
unbounded set and the pod is OOMKilled by anyone who can mail a public inbox.
"""

from __future__ import annotations

from collections.abc import Callable

import curie_mail_adapter.adapter as adapter_module
import pytest
from _support import (
    ALLOWED_SENDER,
    CHANNEL_TOKEN,
    INBOX,
    STRANGER,
    IngressState,
    MailState,
    adapter_env,
    free_port,
    spawn_adapter,
    stop,
    wait_until,
)
from curie_mail_adapter.adapter import MailAdapter


def test_inbound_posts_to_the_platform_ingress(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    mail.add_inbound("msg-1", "thr-1", subject="Quarterly plan", text="please summarize")

    adapter.poll_once()

    assert len(ingress.requests) == 1
    headers, body = ingress.requests[0]
    assert headers["X-API-Key"] == CHANNEL_TOKEN
    # The adapter holds no platform key: only its own scoped channel token.
    assert "Authorization" not in headers
    assert body == {
        "kind": "email",
        "address": INBOX,
        "delivery_id": "msg-1",
        "conversation_id": "thr-1",
        "author": ALLOWED_SENDER,
        "text": "Quarterly plan\n\nplease summarize",
        "reply_ref": "msg-1",
    }


def test_ingress_ids_are_verbatim_agentmail_ids(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """The platform keys idempotency on `delivery_id`, so it must be the upstream id."""
    mail.add_inbound("am-msg-XYZ", "am-thr-ABC")

    adapter.poll_once()

    _headers, body = ingress.requests[0]
    assert body["delivery_id"] == "am-msg-XYZ"
    assert body["conversation_id"] == "am-thr-ABC"
    assert body["reply_ref"] == "am-msg-XYZ"


@pytest.mark.parametrize("status", [200, 202])
def test_a_duplicate_ingress_response_is_not_reposted(
    mail: MailState, ingress: IngressState, adapter: MailAdapter, status: int
) -> None:
    """A response that arrived is final, duplicate or not; only transport failures retry."""
    ingress.response = (status, {"event_id": "chn-1-dup", "stream_id": None, "duplicate": True})
    mail.add_inbound("msg-1", "thr-1")

    adapter.poll_once()

    assert ingress.attempts == 1
    assert len(ingress.requests) == 1


def test_a_transport_failure_is_retried_and_the_delivery_lands_once(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    ingress.drop_next = 2
    mail.add_inbound("msg-1", "thr-1")

    adapter.poll_once()

    assert ingress.attempts == 3
    assert ingress.delivery_ids() == ["msg-1"]


def test_seen_is_bounded_and_evicts_the_oldest_first(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bound is FIFO, and an eviction costs one redundant ingress POST.

    Re-ingesting an evicted message is safe and documented: the platform derives
    the turn's event_id from (binding id, delivery_id) and keeps the claim as a
    permanent receipt, so the redundant POST is answered as a duplicate rather
    than running a second turn.
    """
    monkeypatch.setattr(adapter_module, "SEEN_MAX", 3)
    for index in range(1, 5):
        mail.add_inbound(f"msg-{index}", f"thr-{index}")

    adapter.poll_once()

    assert len(adapter.seen) == 3
    assert "msg-1" not in adapter.seen  # the oldest, evicted first
    assert list(adapter.seen)[-1] == "msg-4"
    assert ingress.delivery_ids() == ["msg-1", "msg-2", "msg-3", "msg-4"]

    adapter.poll_once()

    assert ingress.delivery_ids()[4] == "msg-1"  # the evicted id, re-posted verbatim
    assert len(adapter.seen) == 3


# --- the listing is paginated, and priming is fail-closed ---------------------


def test_a_message_pushed_off_the_first_page_still_reaches_ingress(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """A full page of newer mail must not starve the message behind it.

    AgentMail lists "most recent first" and pages with `next_page_token`
    (https://docs.agentmail.to/api-reference/inboxes/messages/list), so reading
    only the newest POLL_LIMIT and stopping means the newer messages occupy the
    page on every subsequent poll too and the older one is never ingested. That
    is a starvation anyone who can send authenticated mail can trigger at will:
    the flood needs no allow-list entry, because every polled id is only checked
    after it has been read off the page.
    """
    mail.add_inbound("msg-legit", "thr-legit")
    for index in range(adapter_module.POLL_LIMIT):
        mail.add_inbound(f"msg-newer-{index}", f"thr-newer-{index}", sender=STRANGER)

    adapter.poll_once()
    adapter.poll_once()  # the starvation is permanent, not a one-poll delay

    assert "msg-legit" in ingress.delivery_ids()


def test_a_transient_body_fetch_failure_does_not_post_a_subject_only_turn(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """A failed body fetch currently continues with `{}` and posts the subject alone.

    The message is already in `seen` by then, so the agent is handed a
    permanently truncated turn that no later poll repairs. The recovery is the
    half that matters: the full body has to reach ingress once the transient
    failure clears, and exactly once.
    """
    mail.add_inbound("msg-1", "thr-1", subject="Quarterly plan", text="the real body")
    mail.fail_next_body = 500

    adapter.poll_once()
    adapter.poll_once()

    assert [body["text"] for _headers, body in ingress.requests] == [
        "Quarterly plan\n\nthe real body"
    ]


def test_a_message_past_one_passs_page_budget_is_not_starved_forever(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """The page walk is capped per pass, so the next pass has to resume where it stopped.

    One pass reads at most POLL_LIMIT * POLL_MAX_PAGES messages and then throws
    away the `next_page_token` it stopped on
    (https://docs.agentmail.to/api-reference/inboxes/messages/list). Every later
    pass restarts at page one, finds mail it has already seen there, and stops,
    so the message one past the cap is starved permanently rather than delayed a
    pass. The case above uses one page of newer mail and cannot see it: the cap
    is never reached there.

    The flood is sized off the two constants instead of being written as 101, so
    raising the cap only makes the flood bigger and cannot satisfy this test.
    """
    mail.add_inbound("msg-oldest", "thr-oldest", subject="First", text="the starved message")
    for index in range(adapter_module.POLL_LIMIT * adapter_module.POLL_MAX_PAGES):
        mail.add_inbound(f"msg-newer-{index}", f"thr-newer-{index}", sender=STRANGER)

    for _ in range(3):
        adapter.poll_once()

    assert "msg-oldest" in ingress.delivery_ids()


def test_a_body_fetch_that_failed_behind_a_full_page_is_retried(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """Forgetting a message is only a retry if a later pass can still reach it.

    `_forget_seen` drops the id so a later poll can ingest it, but the walk stops
    at the first page carrying anything already seen. A message that failed from
    page two therefore sits behind a full page of admitted mail forever: it is
    released from `seen` and never read again. The transient-body-failure case
    above uses a single message, which is always on page one.
    """
    mail.add_inbound("msg-slow", "thr-slow", subject="Quarterly plan", text="the real body")
    for index in range(adapter_module.POLL_LIMIT):
        mail.add_inbound(f"msg-newer-{index}", f"thr-newer-{index}", sender=STRANGER)
    mail.fail_bodies.add("msg-slow")

    adapter.poll_once()
    assert "msg-slow" not in ingress.delivery_ids()
    mail.fail_bodies.discard("msg-slow")  # the transient provider failure clears

    adapter.poll_once()
    adapter.poll_once()

    assert [body["text"] for _headers, body in ingress.requests] == [
        "Quarterly plan\n\nthe real body"
    ]


def test_a_body_fetch_that_never_succeeds_stops_being_retried(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    """A message the provider will not serve must not own the poller.

    A failed body fetch releases the id from `seen` with no attempt budget and no
    backoff, so the same fetch is reissued on every pass forever. At the 30
    second HTTP timeout in `agentmail.py` a pass holding a hundred such messages
    occupies the sole poller for roughly fifty minutes, delays healthy mail, and
    keeps hammering a provider that may be rate limiting precisely because of it.

    The contract pinned here is behavioral and admits either fix: a bounded
    number of body-fetch attempts per message, or a backoff that engages while
    body fetches are failing. Either way the count has to settle, and at a
    handful of attempts rather than one per pass.
    """
    mail.add_inbound("msg-poison", "thr-poison", subject="Poison", text="never served")
    mail.fail_bodies.add("msg-poison")

    for _ in range(10):
        adapter.poll_once()
    settled = mail.body_calls["msg-poison"]
    for _ in range(5):
        adapter.poll_once()

    assert mail.body_calls["msg-poison"] == settled
    assert settled <= 5, f"the poisoned body was fetched {settled} times and is still growing"

    # And the poller is still available to everyone else.
    mail.add_inbound("msg-healthy", "thr-healthy", subject="Healthy", text="fine")
    adapter.poll_once()

    assert "msg-healthy" in ingress.delivery_ids()


@pytest.mark.parametrize("field", ["extracted_html", "html"])
def test_an_html_only_message_reaches_ingress_with_its_body(
    mail: MailState, ingress: IngressState, adapter: MailAdapter, field: str
) -> None:
    """Reading only `extracted_text` and `text` empties a normal forwarded email.

    "Some email clients - particularly Gmail and Outlook - send forwarded emails
    as HTML-only, with no plain-text part. In these cases, `text` and `preview`
    will be absent", and "always treat `html` as the primary content source and
    `text` as optional". https://docs.agentmail.to/messages
    """
    mail.add_inbound("msg-1", "thr-1", subject="Fwd: Quarterly plan", text=None)
    mail.bodies["msg-1"][field] = "<p>please summarize this</p>"

    adapter.poll_once()

    _headers, body = ingress.requests[0]
    assert "please summarize this" in body["text"]


@pytest.mark.parametrize("list_status", [429, 500, 0])
def test_a_failed_prime_never_ingests_the_backlog(
    mail: MailState, ingress: IngressState, list_status: int
) -> None:
    """Priming is what makes this a live adapter rather than a backfill.

    `prime` logs and returns on a failed listing, leaving `seen` empty, and the
    poll loop then posts everything that arrived while the pod was down as new
    turns: a stale allow-listed message triggers an agent turn on every restart.
    Driven through the real entry point because both fail-closed shapes are
    correct (retry until the listing succeeds, or refuse to start) and only the
    process shows that either one holds.
    """
    port = free_port()
    mail.add_inbound("msg-old", "thr-old")
    mail.fail_next_list = list_status
    proc = spawn_adapter(
        adapter_env(agentmail_base_url=mail.base_url, api_url=ingress.url, port=port)
    )
    try:
        arrived = wait_until(lambda: bool(ingress.requests), 3.0)

        assert not arrived, f"a failed prime replayed the backlog: {ingress.delivery_ids()}"
    finally:
        stop(proc)


def test_rejected_mail_counts_against_the_seen_bound(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """This is the availability case the bound exists for.

    Every polled message_id is inserted before the allow-list decision, so a
    stranger mailing a public inbox grows the set with traffic no inbound gate
    stops.
    """
    monkeypatch.setattr(adapter_module, "SEEN_MAX", 3)
    adapter = make_adapter()
    for index in range(1, 6):
        mail.add_inbound(f"junk-{index}", f"thr-{index}", sender=STRANGER)

    adapter.poll_once()

    assert len(adapter.seen) == 3
    assert ingress.attempts == 0
