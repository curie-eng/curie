"""AgentMail metadata cannot authenticate incoming mail or approval answers.

List/Get Message expose labels and an arbitrary headers map, without a trusted
aligned authentication verdict or a guarantee of header provenance/stripping:
https://docs.agentmail.to/api-reference/inboxes/messages/list
https://docs.agentmail.to/api-reference/inboxes/messages/get
https://docs.agentmail.to/api-reference/webhooks/events/message-received
The inbound authentication policy permits DMARC failure under policy none:
https://docs.agentmail.to/knowledge-base/inbound-emails-missing

These cases drive real provider HTTP and platform HTTP boundaries. Historical
SQLite rows reproduce a store created before the gate changed; they do not
invent a trusted authentication path.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest
from _support import (
    ALLOWED_SENDER,
    STRANGER,
    IngressState,
    MailState,
    adapter_env,
    exit_of,
    free_port,
    spawn_adapter,
    stop,
    wait_for_healthz,
    wait_until,
)
from curie_mail_adapter.adapter import MailAdapter


@pytest.mark.parametrize(
    ("sender", "labels", "headers"),
    [
        (STRANGER, [], None),
        (ALLOWED_SENDER, [], None),
        (ALLOWED_SENDER, ["authenticated", "dmarc_pass", "dkim_pass"], None),
        (
            ALLOWED_SENDER,
            [],
            {
                "Authentication-Results": (
                    "mx.example.com; spf=pass smtp.mailfrom=example.com; "
                    "dkim=pass header.d=example.com; dmarc=pass header.from=example.com"
                ),
                "Received-SPF": "pass",
                "X-AgentMail-Authenticated": "true",
            },
        ),
    ],
    ids=["unlabelled_spoof", "allowlisted_no_verdict", "positive_labels", "forged_headers"],
)
def test_unverifiable_agentmail_authentication_refuses_a_turn(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    caplog: pytest.LogCaptureFixture,
    sender: str,
    labels: list[str],
    headers: dict[str, str] | None,
) -> None:
    summary = mail.add_inbound(
        "msg-unverified", "thr-unverified", sender=sender, labels=labels, headers=headers
    )
    # The listing can supply the same arbitrary provider metadata as Get.
    if headers is not None:
        summary["headers"] = headers
    with caplog.at_level(logging.WARNING):
        assert adapter.poll_once() == 200
        assert adapter.poll_once() == 200

    assert ingress.requests == []
    assert ingress.resolves == []
    assert mail.body_calls == {}
    assert mail.replies == []
    assert adapter.state.delivery("msg-unverified") == {"state": "rejected", "turn": None}
    assert adapter.state.reply_text("thr-unverified", "msg-unverified") == (False, None)
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("curie_mail_adapter") and record.levelno >= logging.WARNING
    ]
    assert len(warnings) == 1
    assert "authentication_unverifiable" in warnings[0]
    assert sender not in warnings[0]
    assert "msg-unverified" not in warnings[0]


@pytest.mark.parametrize("surface", ["prime", "direct"])
def test_each_inbound_surface_applies_the_same_authentication_gate(
    mail: MailState,
    ingress: IngressState,
    adapter: MailAdapter,
    caplog: pytest.LogCaptureFixture,
    surface: str,
) -> None:
    summary = mail.add_inbound("msg-unverified", "thr-unverified")
    with caplog.at_level(logging.WARNING):
        if surface == "prime":
            adapter.prime()
        else:
            assert adapter.state.admit(summary) == "admitted"
            assert adapter.handle_inbound(summary)

    assert ingress.requests == []
    assert mail.body_calls == {}
    assert adapter.state.delivery("msg-unverified") == {"state": "rejected", "turn": None}
    assert "authentication_unverifiable" in caplog.text


@pytest.mark.parametrize("state", ["body_pending", "ingress_pending"])
def test_historical_pending_mail_cannot_bypass_the_new_gate(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    caplog: pytest.LogCaptureFixture,
    state: str,
) -> None:
    original = make_adapter()
    summary = mail.add_inbound("msg-pending", "thr-pending")
    assert original.state.admit(summary) == "admitted"
    if state == "ingress_pending":
        original.state.store_turn(
            "msg-pending",
            {
                "kind": "email",
                "address": original.config.agentmail_inbox,
                "delivery_id": "msg-pending",
                "conversation_id": "thr-pending",
                "author": ALLOWED_SENDER,
                "text": "Previously stored, never authenticated",
                "reply_ref": "msg-pending",
            },
        )
    original.close()
    replacement = make_adapter()
    try:
        with caplog.at_level(logging.WARNING):
            replacement.poll_once()
        assert ingress.requests == []
        assert ingress.resolves == []
        assert mail.body_calls == {}
        assert replacement.state.pending() == []
        assert replacement.state.delivery("msg-pending") == {"state": "rejected", "turn": None}
        assert replacement.state.live_reply_refs("thr-pending") == []
        assert "authentication_unverifiable" in caplog.text
    finally:
        replacement.close()


def test_an_email_approval_answer_without_verifiable_authentication_is_refused(
    mail: MailState,
    ingress: IngressState,
    make_adapter: Callable[..., MailAdapter],
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter = make_adapter(adapter_principal="adp.example.example")
    reference = "curie-approval-" + "a" * 24
    adapter.state.issue_approval_ref(
        "appr-1",
        "thr-1",
        "msg-historical",
        reference,
        requester=ALLOWED_SENDER,
        approvers=(ALLOWED_SENDER,),
    )
    mail.add_inbound(
        "msg-answer",
        "thr-1",
        sender=ALLOWED_SENDER,
        text="APPROVE",
        full_text=f"APPROVE\nApproval reference: {reference}",
        headers={
            "From": ALLOWED_SENDER,
            "Message-ID": "<answer@example.com>",
            "Authentication-Results": "mx.example.com; dmarc=pass header.from=example.com",
        },
    )
    try:
        with caplog.at_level(logging.WARNING):
            adapter.poll_once()
        assert ingress.resolves == []
        assert ingress.requests == []
        assert mail.replies == []
        assert mail.body_calls == {}
        stored_reference = adapter.state.approval_ref_for("appr-1")
        assert stored_reference is not None
        assert stored_reference["state"] == "live"
        assert adapter.state.delivery("msg-answer") == {"state": "rejected", "turn": None}
        assert "authentication_unverifiable" in caplog.text
    finally:
        adapter.close()


@pytest.mark.parametrize("opt_in", [None, "false"])
@pytest.mark.parametrize("senders", ["*", f"{ALLOWED_SENDER},*"])
@pytest.mark.parametrize("ingress_enabled", ["true", "false"])
def test_wildcard_sender_filter_requires_explicit_boot_opt_in(
    mail: MailState,
    ingress: IngressState,
    opt_in: str | None,
    senders: str,
    ingress_enabled: str,
) -> None:
    overrides = {} if opt_in is None else {"CURIE_MAIL_ALLOW_ALL_SENDERS": opt_in}
    env = adapter_env(
        agentmail_base_url=mail.base_url,
        api_url=ingress.url,
        port=free_port(),
        allowed_senders=senders,
        ingress_enabled=ingress_enabled,
        **overrides,
    )

    code, output = exit_of(spawn_adapter(env), timeout=5)

    assert code != 0
    assert "CURIE_MAIL_ALLOW_ALL_SENDERS" in output


def test_wildcard_boot_opt_in_never_authenticates_incoming_mail(
    mail: MailState, ingress: IngressState
) -> None:
    port = free_port()
    proc = spawn_adapter(
        adapter_env(
            agentmail_base_url=mail.base_url,
            api_url=ingress.url,
            port=port,
            allowed_senders="*",
            CURIE_MAIL_ALLOW_ALL_SENDERS="true",
        )
    )
    try:
        wait_for_healthz(port, proc)
        assert wait_until(lambda: mail.list_calls > 0)
        previous_calls = mail.list_calls
        mail.add_inbound("msg-after-boot", "thr-after-boot", sender=STRANGER)
        assert wait_until(lambda: mail.list_calls >= previous_calls + 2)
    finally:
        output = stop(proc)

    assert ingress.requests == []
    assert ingress.resolves == []
    assert mail.body_calls == {}
    assert "authentication_unverifiable" in output


def test_empty_sender_filter_boot_error_does_not_recommend_a_wildcard(
    mail: MailState, ingress: IngressState
) -> None:
    code, output = exit_of(
        spawn_adapter(
            adapter_env(
                agentmail_base_url=mail.base_url,
                api_url=ingress.url,
                port=free_port(),
                allowed_senders="",
            )
        )
    )
    assert code != 0
    assert "CURIE_MAIL_ALLOWED_SENDERS" in output
    assert "*" not in output
