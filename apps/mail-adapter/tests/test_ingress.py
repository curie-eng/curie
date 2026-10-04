"""Discovery fails closed; the independent channel POST transport remains tested.

No AgentMail body or turn is admitted without verifiable authentication.
Pagination, bounded receipts, prime behavior, and lower level HTTP response
classification remain covered through their real boundaries.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

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

REFUSAL_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "channel-port-refusal.json"
)


def _turn(message_id: str = "msg-1", conversation_id: str = "thr-1") -> dict[str, str]:
    """A transport payload, never an assertion that AgentMail admitted this turn."""
    return {
        "kind": "email",
        "address": INBOX,
        "delivery_id": message_id,
        "conversation_id": conversation_id,
        "author": ALLOWED_SENDER,
        "text": "Quarterly plan\n\nplease summarize",
        "reply_ref": message_id,
    }


def _refusal() -> tuple[int, dict[str, str]]:
    vector = json.loads(REFUSAL_VECTOR.read_text())
    assert set(vector) == {"comment", "status", "detail"}
    return int(vector["status"]), {"detail": str(vector["detail"])}


def test_channel_post_transport_uses_only_its_scoped_credential_and_verbatim_ids(
    ingress: IngressState, adapter: MailAdapter
) -> None:
    turn = _turn("am-msg-XYZ", "am-thr-ABC")
    assert adapter.post_turn(turn) == "accepted"
    ((headers, body),) = ingress.requests
    assert headers["X-API-Key"] == CHANNEL_TOKEN
    assert "Authorization" not in headers
    assert body == turn


@pytest.mark.parametrize("duplicate", [False, True])
def test_channel_post_transport_classifies_terminal_200(
    ingress: IngressState, adapter: MailAdapter, duplicate: bool
) -> None:
    ingress.response = (200, {"event_id": "chn-1", "stream_id": None, "duplicate": duplicate})
    assert adapter.post_turn(_turn()) == "accepted"
    assert ingress.attempts == 1


@pytest.mark.parametrize(
    ("status", "headers"),
    [(202, {}), (401, {}), (429, {"Retry-After": "0.05"}), (500, {})],
)
def test_channel_post_transport_preserves_retry_classification_and_payload(
    ingress: IngressState, adapter: MailAdapter, status: int, headers: dict[str, str]
) -> None:
    ingress.responses = [
        (status, {"detail": "retry"}, headers),
        (200, {"event_id": "chn-1", "duplicate": False}, {}),
    ]
    assert adapter.post_turn(_turn()) == "retry"
    assert adapter.post_turn(_turn()) == "accepted"
    assert ingress.delivery_ids() == ["msg-1", "msg-1"]
    if status == 429:
        assert ingress.attempt_times[1] - ingress.attempt_times[0] >= 0.045


def test_channel_post_transport_retries_connection_failures_with_a_bound(
    ingress: IngressState, adapter: MailAdapter
) -> None:
    ingress.drop_next = 2
    assert adapter.post_turn(_turn()) == "accepted"
    assert ingress.attempts == 3
    assert ingress.delivery_ids() == ["msg-1"]


def test_channel_post_transport_classifies_the_exact_caller_refusal(
    ingress: IngressState, adapter: MailAdapter, caplog: pytest.LogCaptureFixture
) -> None:
    ingress.response = _refusal()
    with caplog.at_level("WARNING", logger="curie_mail_adapter.adapter"):
        assert adapter.post_turn(_turn()) == "refused"
    assert ingress.attempts == 1
    refusal_logs = [r.getMessage() for r in caplog.records if "ingress refused" in r.getMessage()]
    assert len(refusal_logs) == 1
    assert ALLOWED_SENDER not in refusal_logs[0] and "msg-1" not in refusal_logs[0]


@pytest.mark.parametrize(
    "response",
    [
        (403, "<html>Forbidden</html>"),
        (403, {"detail": "Forbidden"}),
        (403, {"detail": "caller_not_allowed_by_proxy"}),
        (403, {"detail": {"code": "caller_not_allowed"}}),
        (403, {}),
    ],
)
def test_channel_post_transport_keeps_other_403_responses_retryable(
    ingress: IngressState, adapter: MailAdapter, response: tuple[int, object]
) -> None:
    ingress.response = response  # type: ignore[assignment]
    assert adapter.post_turn(_turn()) == "retry"
    assert ingress.attempts == 1


def test_seen_is_bounded_without_reposting_rejected_mail(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(adapter_module, "SEEN_MAX", 3)
    for index in range(1, 5):
        mail.add_inbound(f"msg-{index}", f"thr-{index}")
    adapter.poll_once()
    assert len(adapter.seen) == 3
    assert "msg-1" not in adapter.seen
    assert list(adapter.seen)[-1] == "msg-4"
    adapter.poll_once()
    assert ingress.delivery_ids() == []
    assert mail.body_calls == {}
    assert len(adapter.seen) == 3


@pytest.mark.parametrize("pages", [1, adapter_module.POLL_MAX_PAGES])
def test_discovery_reaches_and_rejects_a_message_behind_newer_pages(
    mail: MailState, ingress: IngressState, adapter: MailAdapter, pages: int
) -> None:
    # List Messages lists newest first and uses next_page_token:
    # https://docs.agentmail.to/api-reference/inboxes/messages/list
    mail.add_inbound("msg-oldest", "thr-oldest")
    for index in range(adapter_module.POLL_LIMIT * pages):
        mail.add_inbound(f"msg-newer-{index}", f"thr-newer-{index}", sender=STRANGER)
    for _ in range(3):
        adapter.poll_once()
    assert adapter.state.delivery("msg-oldest") == {"state": "rejected", "turn": None}
    assert ingress.delivery_ids() == []
    assert mail.body_calls == {}


def test_invalid_page_token_recovers_without_losing_rejection_receipts(
    mail: MailState, ingress: IngressState, adapter: MailAdapter
) -> None:
    mail.add_inbound("msg-unclaimed", "thr-unclaimed")
    mail.next_page_token_override_once = "provider-rejects-this-token"
    mail.invalid_page_tokens.add("provider-rejects-this-token")
    adapter.poll_once()
    adapter.poll_once()
    assert adapter.state.delivery("msg-unclaimed") == {"state": "rejected", "turn": None}
    assert ingress.delivery_ids() == []
    assert mail.body_calls == {}


@pytest.mark.parametrize("body_shape", ["plain", "html", "failed", "oversize"])
def test_unverified_mail_is_rejected_before_body_allocation_or_retry(
    mail: MailState, ingress: IngressState, adapter: MailAdapter, body_shape: str
) -> None:
    mail.add_inbound("msg-1", "thr-1", text=None if body_shape == "html" else "body")
    if body_shape == "html":
        mail.bodies["msg-1"]["extracted_html"] = "<p>body</p>"
    elif body_shape == "failed":
        mail.fail_bodies.add("msg-1")
    elif body_shape == "oversize":
        mail.bodies["msg-1"]["extracted_text"] = "x" * (adapter.config.max_body_bytes + 1)
    for _ in range(10):
        adapter.poll_once()
    assert mail.body_calls == {}
    assert ingress.delivery_ids() == []
    assert adapter.state.pending() == []
    assert adapter.state.delivery("msg-1") == {"state": "rejected", "turn": None}


@pytest.mark.parametrize("list_status", [429, 500, 0])
def test_a_failed_prime_never_ingests_the_backlog(
    mail: MailState, ingress: IngressState, list_status: int
) -> None:
    port = free_port()
    mail.add_inbound("msg-old", "thr-old")
    mail.fail_next_list = list_status
    proc = spawn_adapter(
        adapter_env(agentmail_base_url=mail.base_url, api_url=ingress.url, port=port)
    )
    try:
        assert not wait_until(lambda: bool(ingress.requests), 3.0)
    finally:
        stop(proc)


def test_pending_capacity_does_not_turn_unverified_mail_into_a_claim(
    mail: MailState, ingress: IngressState, make_adapter: Callable[..., MailAdapter]
) -> None:
    adapter = make_adapter(max_pending_deliveries=1)
    historical = {"message_id": "msg-pending", "thread_id": "thr-pending", "labels": []}
    assert adapter.state.admit(historical) == "admitted"
    mail.add_inbound("msg-new", "thr-new")
    adapter.poll_once()
    assert ingress.delivery_ids() == []
    assert mail.body_calls == {}
    assert adapter.state.pending() == []
    assert adapter.state.delivery("msg-pending") == {"state": "rejected", "turn": None}
    assert adapter.state.delivery("msg-new") == {"state": "rejected", "turn": None}
