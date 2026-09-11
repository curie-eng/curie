"""What the poller does when the provider connection drops on the listing.

Two properties, both observed at the real external seam: the CAUSE the adapter
records for a failed discovery pass, and the CADENCE at which it retries after
one. A 24h soak of the deployed adapter produced 373 identical
`poll: list failed with status=0` warnings (#2012) — the numeric status was the
whole record, so the outage was undiagnosable from the log, and because status 0
reset the backoff the poller kept hammering the dead endpoint at its normal
interval. A later soak found the same cadence problem for persistent HTTP 403
responses (#2555). The fixes expose only bounded, locally synthesized causes and
arm the existing bounded backoff for transport failures and HTTP refusals; these
tests pin both halves, plus the reset that must still happen on recovery.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

import curie_mail_adapter.adapter as adapter_module
import pytest
from _support import (
    AGENTMAIL_API_KEY,
    CHANNEL_TOKEN,
    EGRESS_SECRET,
    IngressState,
    MailState,
    wait_until,
)
from curie_mail_adapter.adapter import MailAdapter

FAILURE_PREFIX = "poll: list failed"


def _failure_record(caplog: pytest.LogCaptureFixture) -> str:
    """The single discovery-failure warning, as the operator would read it."""
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith(FAILURE_PREFIX)
    ]
    assert len(messages) == 1, f"expected exactly one {FAILURE_PREFIX!r} warning: {messages}"
    return messages[0]


def test_a_dropped_list_call_records_a_bounded_safe_cause(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A transport failure is logged with its cause, bounded and credential-free.

    A numeric status alone is what made the soak undiagnosable: DNS failure,
    refused connection, TLS error and read timeout all arrive as status 0, and
    the structured body that distinguishes them was being discarded at the one
    place an operator would look. Reading it back is the fix; the character
    budget is what keeps a hostile or merely verbose provider string from
    becoming the log line, and the redaction pass is why a cause can be logged
    at all without risking the adapter's three credentials.
    """
    mail.drop_next_lists = 1

    with caplog.at_level(logging.WARNING, logger="curie_mail_adapter.adapter"):
        status = adapter.poll_once()

    assert status == 0, f"a dropped connection must surface as a transport failure, got {status}"
    message = _failure_record(caplog)
    cause = message.partition("cause=")[2]
    assert cause, f"the failure warning carries no cause at all: {message!r}"
    assert cause != "unavailable", (
        f"the structured provider body was discarded rather than read: {message!r}"
    )
    for credential in (AGENTMAIL_API_KEY, CHANNEL_TOKEN, EGRESS_SECRET):
        assert credential not in message, f"a credential reached the log: {message!r}"
    assert len(cause) <= adapter_module.CAUSE_MAX_CHARS + 3, (
        f"the cause is unbounded at {len(cause)} characters: {cause!r}"
    )


def test_a_long_credential_bearing_cause_is_redacted_and_truncated(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
) -> None:
    """The redaction pass and the character budget, on input the socket cannot produce.

    A dropped connection only ever yields a short, generic ``OSError`` string
    carrying none of the three credentials, so the end-to-end test above can
    assert "no credential in the log" and "the cause is bounded" while neither
    the redaction loop nor the ``CAUSE_MAX_CHARS`` truncation ever runs: both of
    those assertions hold just as well with the two passes deleted, and an
    assertion that cannot fail is not protection. Both passes exist for a failure
    mode the real transport cannot be made to produce on demand - a stdlib
    rendering that starts carrying the request header, or a provider socket error
    a kilobyte long - so the hostile body is handed to the helper directly.

    That is not the internal patching `CLAUDE.md` forbids. Nothing is replaced or
    faked; both fake servers still stand behind the fixtures. The helper is a
    pure function of its two arguments, and passing them is the only way to hand
    it an input the real seam refuses to generate.
    """
    leaked = (
        f"<urlopen error [Errno 111] key={AGENTMAIL_API_KEY} "
        f"chn={CHANNEL_TOKEN} egr={EGRESS_SECRET} " + "padding " * 40 + ">"
    )
    # Guard the fixture itself. The credentials must sit before the truncation
    # point, or truncation rather than redaction is what removes them and the
    # assertions below go vacuous in the other direction.
    assert len(leaked) > adapter_module.CAUSE_MAX_CHARS, (
        f"the hostile cause stopped being long enough to truncate: {len(leaked)} chars"
    )
    for credential in (AGENTMAIL_API_KEY, CHANNEL_TOKEN, EGRESS_SECRET):
        position = leaked.find(credential)
        assert 0 <= position < adapter_module.CAUSE_MAX_CHARS, (
            f"{credential!r} sits at {position} rather than before the truncation point, "
            f"so this test no longer exercises redaction: {leaked!r}"
        )

    cause = adapter._transport_cause(0, {"error": leaked})

    for credential in (AGENTMAIL_API_KEY, CHANNEL_TOKEN, EGRESS_SECRET):
        assert credential not in cause, f"a credential survived redaction: {cause!r}"
    assert "[redacted]" in cause, f"the redaction pass never ran: {cause!r}"
    assert len(cause) <= adapter_module.CAUSE_MAX_CHARS + 3, (
        f"the cause is unbounded at {len(cause)} characters: {cause!r}"
    )
    assert cause.endswith("..."), f"the character budget never truncated: {cause!r}"
    assert adapter._transport_cause(500, {"error": leaked}) == "http_500", (
        "a nonzero status was not rendered as its bounded status-only token"
    )


@pytest.mark.parametrize("http_status", [403, 500])
def test_a_provider_error_body_is_replaced_by_a_bounded_status_token(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    caplog: pytest.LogCaptureFixture,
    http_status: int,
) -> None:
    """A non-200 body is provider-authored, so the adapter must not read it at all.

    The failure branch fires for every non-200 status, not only the transport
    failure, and on a 4xx/5xx the provider's own parsed body is what reaches the
    cause helper. That body is arbitrary: a provider is free to echo the subject
    and text of the very mail being listed under an ``error`` key, which is the
    shape the helper used to render. The character budget is no defence there -
    a truncated payroll subject is still mail content in the cluster's log
    retention - so the only sound rule is that a body this package did not write
    is never read. Only a status 0 body is locally synthesized, so status alone
    decides, and every other failure is recorded by its status code.
    """
    mail.injected_body = {"error": "Subject: payroll spreadsheet; body: SSN 123-45-6789"}
    mail.fail_next_list = http_status

    with caplog.at_level(logging.WARNING, logger="curie_mail_adapter.adapter"):
        observed_status = adapter.poll_once()

    assert observed_status == http_status, (
        f"the injected provider failure did not surface, got {observed_status}"
    )
    message = _failure_record(caplog)
    cause = message.partition("cause=")[2]
    assert cause == f"http_{http_status}", (
        f"the provider failure was not reduced to its status-only token: {message!r}"
    )
    assert len(cause) <= adapter_module.CAUSE_MAX_CHARS, f"cause is unbounded: {cause!r}"
    assert "SSN" not in message, f"mail content reached the log: {message!r}"
    assert "payroll" not in message, f"mail content reached the log: {message!r}"


@pytest.mark.parametrize("refusal_status", [401, 403, 429])
def test_persistent_http_refusals_back_off_then_200_resets_cadence(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    monkeypatch: pytest.MonkeyPatch,
    refusal_status: int,
) -> None:
    """Repeated HTTP refusals slow discovery; the first 200 restores cadence.

    The fake's HTTP fault is one-shot, so the test re-arms it during the delay
    that follows each refusal. Every observed status still comes from a real
    request to the local provider server. Parametrizing 403 with a sibling auth
    refusal and the already backed-off 429 case protects the full 4xx rule.
    """
    monkeypatch.setattr(adapter_module, "BACKOFF_STEP_SECONDS", 0.2)
    monkeypatch.setattr(adapter_module, "BACKOFF_MAX_SECONDS", 0.6)
    adapter = make_adapter(poll_interval_seconds=0.01)
    thread = threading.Thread(target=adapter.poll_loop, daemon=True)
    refusal_indexes: list[int] = []

    try:
        thread.start()
        assert adapter.ready.wait(10), "prime never completed against an empty inbox"
        for _ in range(3):
            mail.fail_next_list = refusal_status
            assert wait_until(lambda: mail.fail_next_list is None, timeout=5), (
                f"the fake never served HTTP {refusal_status}"
            )
            refusal_indexes.append(len(mail.list_times) - 1)

        recovery = refusal_indexes[-1] + 1
        assert wait_until(lambda: len(mail.list_times) >= recovery + 3, timeout=10), (
            "the poller never made enough successful passes to expose the 200 reset"
        )
    finally:
        adapter.shutdown.set()
        thread.join(timeout=10)

    times = mail.list_times
    assert refusal_indexes == list(range(refusal_indexes[0], refusal_indexes[0] + 3)), (
        f"the repeated refusals were not consecutive provider calls: {refusal_indexes}"
    )
    waits_after_refusals = [
        times[refusal_indexes[index] + 1] - times[refusal_indexes[index]]
        for index in range(len(refusal_indexes))
    ]
    assert waits_after_refusals[1] > waits_after_refusals[0] + 0.15, (
        f"HTTP {refusal_status} did not grow the backoff: {waits_after_refusals}"
    )
    assert waits_after_refusals[-1] <= 0.61 + 0.25, (
        f"HTTP {refusal_status} grew past the bounded ceiling: {waits_after_refusals}"
    )
    assert times[recovery + 2] - times[recovery + 1] < 0.15, (
        f"a 200 did not clear the HTTP {refusal_status} backoff"
    )


def test_a_5xx_does_not_arm_backoff(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5xx from an unarmed poller retains its documented normal cadence."""
    monkeypatch.setattr(adapter_module, "BACKOFF_STEP_SECONDS", 0.2)
    adapter = make_adapter(poll_interval_seconds=0.01)
    thread = threading.Thread(target=adapter.poll_loop, daemon=True)

    try:
        thread.start()
        assert adapter.ready.wait(10), "prime never completed against an empty inbox"
        mail.fail_next_list = 500
        assert wait_until(lambda: mail.fail_next_list is None, timeout=5), (
            "the fake never served the 500 control"
        )
        failed_500 = len(mail.list_times) - 1
        assert wait_until(lambda: len(mail.list_times) >= failed_500 + 3, timeout=5), (
            "the poller never made a pass after the 500 control"
        )
    finally:
        adapter.shutdown.set()
        thread.join(timeout=10)

    assert mail.list_times[failed_500 + 1] - mail.list_times[failed_500] < 0.15, (
        "a 5xx armed discovery backoff even though that behavior remains out of scope"
    )


def test_repeated_list_transport_failures_back_off_then_reset_on_recovery(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sustained outage slows the poller down, and recovery restores cadence.

    The two backoff bounds are scaled down for this test: production takes a 5s
    step to a 60s ceiling, so watching three growth steps at production values
    would cost minutes of suite time for a property that is about ordering, not
    absolute seconds. Only those two numeric bounds are rebound, for the
    duration of this test: no function, class or I/O path inside
    `curie_mail_adapter` is replaced, and every request still crosses the real
    fake AgentMail server, so the cadence under test is the production one,
    observed as wall-clock arrival times of real requests at the provider seam.

    The assertions are ordering and bounds rather than exact values, because the
    suite shares a box with whatever else is running on it. They are keyed to the
    failure POSITIONS the fake reports rather than to elapsed-time
    classification: on a loaded runner a normal poll can be descheduled past any
    "this gap looks slow" threshold, and a test that infers which gaps were
    backed-off ones from their duration mistakes that deschedule for the very
    property it is pinning.
    """
    monkeypatch.setattr(adapter_module, "BACKOFF_STEP_SECONDS", 0.2)
    monkeypatch.setattr(adapter_module, "BACKOFF_MAX_SECONDS", 0.6)
    adapter = make_adapter(poll_interval_seconds=0.01)
    thread = threading.Thread(target=adapter.poll_loop, daemon=True)

    try:
        thread.start()
        assert adapter.ready.wait(10), "prime never completed against an empty inbox"
        baseline = len(mail.list_times)
        # Arm the outage BEFORE seeding, so the message cannot be delivered by a
        # pass that races the arming and the whole outage sits between us and it.
        mail.drop_next_lists = 4
        mail.add_inbound("msg-drop", "thr-drop", subject="Held", text="delivered after the outage")

        assert wait_until(lambda: ingress.delivery_ids() == ["msg-drop"], timeout=20), (
            f"the held message never survived the outage: {ingress.delivery_ids()}"
        )
        # Keep polling past the recovered pass, so a post-recovery gap exists to
        # judge the reset by rather than inferring it from the recovery itself.
        assert wait_until(lambda: len(mail.list_times) >= baseline + 9, timeout=20), (
            "the poller never resumed enough passes to expose a post-recovery gap"
        )
    finally:
        adapter.shutdown.set()
        thread.join(timeout=10)

    times = mail.list_times[baseline:]
    # The fake records which list calls it dropped, so the waits that followed a
    # failure are known rather than guessed. Its indexes are absolute; shift them
    # into this run's window.
    dropped = [index - baseline for index in mail.dropped_list_indexes if index >= baseline]
    # `times[k]` is call k's arrival, so the delay the adapter waited before a
    # dropped call is the interval immediately preceding it.
    waits_before_failures = [times[index] - times[index - 1] for index in dropped if index >= 1]

    assert len(dropped) == 4, (
        f"the outage the test armed is not the outage that happened: {dropped}"
    )
    assert len(waits_before_failures) >= 3, (
        f"too few known-failure waits to judge growth: {waits_before_failures}"
    )
    assert waits_before_failures[1] > waits_before_failures[0] + 0.15, (
        f"the delay did not grow across successive failures: {waits_before_failures}"
    )
    # 0.61 is the scaled ceiling plus one poll interval, the largest wait a clamped
    # backoff can produce; the quarter second is scheduler slack. A tolerance wider
    # than the ceiling itself would still pass with the clamp deleted.
    assert max(waits_before_failures) <= 0.61 + 0.25, (
        f"the backoff grew past its ceiling instead of saturating: {waits_before_failures}"
    )
    # This is the load-bearing check on the clamp. Once the ceiling is reached the
    # wait stops changing, so two consecutive equal waits are the clamp's signature;
    # unclamped, those two differ by roughly a doubling. The drops are consecutive
    # list calls, so this list is deterministic in shape - the waits preceding drops
    # 2, 3 and 4 - however many normal passes raced the arming, and the `>= 3` check
    # above already guarantees both indexes exist. Do not relax it back into a broad
    # tolerance: a tolerance cannot distinguish saturation from continued growth.
    assert abs(waits_before_failures[-1] - waits_before_failures[-2]) < 0.15, (
        "the backoff kept doubling instead of saturating at its ceiling: "
        f"{waits_before_failures}"
    )
    # The recovery pass is the first list call after the last dropped one, so the
    # final interval of the run is fully post-recovery and needs no guessing.
    recovery = dropped[-1] + 1
    assert len(times) >= recovery + 3, (
        f"too few post-recovery passes to judge the reset: {len(times)} after {recovery}"
    )
    assert times[-1] - times[-2] < 0.15, (
        f"the normal cadence never resumed after recovery: {times[-1] - times[-2]}"
    )
    assert ingress.delivery_ids() == ["msg-drop"], (
        f"the held message was skipped or duplicated across the outage: {ingress.delivery_ids()}"
    )


def test_an_unrelated_failure_does_not_clear_an_armed_backoff(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5xx in the middle of a transport outage must not reset the cadence.

    A provider outage is rarely one failure mode end to end: a dropped
    connection and a 5xx interleave as the provider's edge fails in different
    ways, and the poller sees the mixture, not a clean run of one status. If the
    5xx un-arms the delay the two drops just armed, the poller returns to its
    normal interval in the MIDDLE of the outage and resumes the warning burst
    this issue is about — a bug no single-failure-mode test can see, because
    each status is handled correctly on its own.

    The two fault mechanisms are armed together before the first affected call:
    the fake handler consumes `drop_next_lists` ahead of the one-shot
    `fail_next_list`, so setting both is atomic and produces exactly drop, drop,
    500, normal service. Nothing has to be timed against a live loop, so there
    is no window in which a racing pass could reorder the outage. The bounds are
    scaled exactly as the cadence test above scales them, and for the same
    reason; every request still crosses the real fake AgentMail server.
    """
    monkeypatch.setattr(adapter_module, "BACKOFF_STEP_SECONDS", 0.2)
    monkeypatch.setattr(adapter_module, "BACKOFF_MAX_SECONDS", 0.6)
    adapter = make_adapter(poll_interval_seconds=0.01)
    thread = threading.Thread(target=adapter.poll_loop, daemon=True)

    try:
        thread.start()
        assert adapter.ready.wait(10), "prime never completed against an empty inbox"
        baseline = len(mail.list_times)
        mail.drop_next_lists = 2
        mail.fail_next_list = 500
        mail.add_inbound(
            "msg-mixed", "thr-mixed", subject="Mixed", text="delivered after the outage"
        )

        assert wait_until(lambda: ingress.delivery_ids() == ["msg-mixed"], timeout=20), (
            f"the held message never survived the mixed outage: {ingress.delivery_ids()}"
        )
        # Keep polling past recovery, so the interval after the 500 is a settled
        # measurement rather than the tail of the run.
        assert wait_until(lambda: len(mail.list_times) >= baseline + 8, timeout=20), (
            "the poller never resumed enough passes to expose the post-500 interval"
        )
    finally:
        adapter.shutdown.set()
        thread.join(timeout=10)

    times = mail.list_times[baseline:]
    dropped = [index - baseline for index in mail.dropped_list_indexes if index >= baseline]
    # The 500 is answered by the call right after the last dropped one, and the
    # first successful listing is the one after that. The wait the adapter took
    # BEFORE that successful call is the whole question: the backoff the two
    # drops armed must still have been in force across the 500.
    failed_500 = dropped[-1] + 1
    wait_before_recovery = times[failed_500 + 1] - times[failed_500]

    assert len(dropped) == 2, (
        f"the outage the test armed is not the outage that happened: {dropped}"
    )
    assert len(times) > failed_500 + 1, (
        f"no listing after the 500 to measure: {len(times)} calls, 500 at {failed_500}"
    )
    # Held at its 0.6s ceiling the wait is about 0.61s; if the 500 cleared the
    # backoff it is the bare 0.01s poll interval. 0.3 sits far from both, so the
    # check cannot be satisfied by scheduler slack in either direction.
    assert wait_before_recovery > 0.3, (
        f"an unrelated 5xx cleared the armed transport backoff: {wait_before_recovery}"
    )
    assert ingress.delivery_ids() == ["msg-mixed"], (
        f"the held message was skipped or duplicated across the outage: {ingress.delivery_ids()}"
    )
