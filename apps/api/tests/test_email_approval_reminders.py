"""A completed mail turn keeps its original approval answer authority. ADR 0273."""

from __future__ import annotations

import asyncio
import base64
import importlib
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest
import redis
from channel_protocol.reply import ReplyPost
from curie_api.config import get_settings
from curie_internal.approval_reminder import ApprovalReminderV1
from curie_internal.keyspace import cluster_reply_keys
from curie_mail_adapter.adapter import APPROVAL_INSTRUCTIONS, APPROVAL_REF_PATTERN, MailAdapter
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.egress import make_server
from curie_worker.approval_cards import ApprovalCardStore
from curie_worker.approval_reminders import deliver_reminder
from curie_worker.approvals import ApprovalClient
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import (
    CLUSTER_MESSAGE_ADAPTER,
    HttpReplyAdapter,
    TargetRoute,
    build_reply_sink,
)
from redis.asyncio import Redis

from apps.api.tests.test_http_approval_answers import GROUP, approver_role, resolve
from apps.api.tests.test_http_approval_creation import rows
from apps.api.tests.test_http_approval_legacy_writers import created
from apps.api.tests.test_http_email_approvals import email_answer
from apps.api.tests.test_http_email_approvals import email_case as email_case
from apps.api.tests.test_http_email_receivers import (
    EMAIL_KEY,
    INBOX,
    MAIL_ADAPTER,
    SENDER,
)
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

mail_support = importlib.import_module("apps.mail-adapter.tests._support")
verifying = importlib.import_module("apps.mail-adapter.tests._verifying_provider")


@pytest.mark.parametrize("eligible", [True, False])
def test_completed_mail_turn_gets_reminder_then_original_signed_answer(
    email_case: SimpleNamespace,
    valkey: redis.Redis,
    runs_stream: str,
    tmp_path: Path,
    eligible: bool,
) -> None:
    case = email_case
    case.row = created(case)
    approval_id = str(case.row["id"])
    conversation = case.row["conversation_id"]
    approver_role(case, group=GROUP if eligible else "SyntheticApproverOnlyGroup")
    answers: list[tuple[int, Any]] = []

    class ApiTransport(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            pass

        def forward(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            response = case.client.request(
                self.command,
                self.path,
                content=body,
                headers={
                    key: value for key, value in self.headers.items() if key.lower() != "host"
                },
            )
            if self.path.endswith("/resolve"):
                answers.append((response.status_code, response.json()))
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = forward
        do_POST = forward

    mail = mail_support.MailState()
    provider_server = mail_support.serve(mail_support.MailHandler, mail)
    api_server = ThreadingHTTPServer(("127.0.0.1", 0), ApiTransport)
    api_thread = threading.Thread(target=api_server.serve_forever, daemon=True)
    api_thread.start()
    _, headers = email_answer(case)
    config = MailAdapterConfig(
        agentmail_api_key=mail_support.AGENTMAIL_API_KEY,
        agentmail_inbox=INBOX,
        agentmail_base_url=f"http://127.0.0.1:{provider_server.server_port}/v0",
        api_base_url=f"http://127.0.0.1:{api_server.server_port}",
        channel_token=case.email_channel_token,
        adapter_principal=headers["X-Curie-Adapter-Principal"],
        egress_secret=mail_support.EGRESS_SECRET,
        allowed_senders=(SENDER,),
        receiver_id=case.email["receiver_id"],
        receiver_generation=1,
        receiver_key=base64.urlsafe_b64encode(EMAIL_KEY).rstrip(b"=").decode(),
        state_path=str(tmp_path / "mail.sqlite3"),
        ingress_enabled=True,
    )
    adapter = MailAdapter(config, provider=verifying.VerifyingTestProvider(config))
    egress = make_server(adapter, 0)
    egress_thread = threading.Thread(target=egress.serve_forever, daemon=True)
    egress_thread.start()
    endpoint = f"http://127.0.0.1:{egress.server_port}/"
    requester = "requester@example.com"
    try:
        mail_support.seed_historical_reply(
            mail,
            adapter.state,
            "msg-original",
            conversation,
            sender=requester,
            cc=[SENDER],
        )

        def post(event: dict[str, Any]) -> dict[str, Any]:
            event["target"]["address"] = INBOX
            status, ack = mail_support.post_event(endpoint, event)
            assert status == 200, ack
            return ack

        ack = post(
            mail_support.approval_card(
                approval_id,
                conversation_id=conversation,
                requested_by=requester,
                approvers=[SENDER],
            )
        )
        post(
            mail_support.completed(
                "ev-original",
                conversation_id=conversation,
                reply_ref="msg-original",
                outcome="awaiting-approval",
            )
        )
        assert adapter.state.live_reply_refs(conversation) == []
        original = mail.replies_to("msg-original")
        assert len(original) == 1
        assert mail.deliveries[-1][1] == frozenset({requester, SENDER})
        (reference,) = APPROVAL_REF_PATTERN.findall(original[0])
        before_stream = valkey.xrange(runs_stream)
        before_lookups = len(case.provider.requests)

        async def notify() -> None:
            client = Redis.from_url(get_settings().valkey_dsn(), decode_responses=True)
            sink = HttpReplyAdapter({MAIL_ADAPTER: mail_support.EGRESS_SECRET})
            relay = build_reply_sink(
                WorkerConfig(
                    api_base_url=config.api_base_url,
                    internal_worker_token=get_settings().internal_worker_token,
                )
            )
            try:
                cards = ApprovalCardStore(client, WorkerConfig())
                relay_ref = str(uuid4())
                route = TargetRoute(adapter=CLUSTER_MESSAGE_ADAPTER)
                event = mail_support.reply_post("Original card", conversation_id=conversation)
                event["target"]["reply_ref"] = relay_ref
                await relay.emit(ReplyPost.model_validate(event), route=route)
                keys = cluster_reply_keys(relay_ref)
                for key in keys:
                    await client.pexpire(key, 600_000)
                await cards.remember(
                    approval_id,
                    channel="C0EXAMPLE1",
                    ts=relay_ref,
                    summary="Send the quote",
                    endpoint=None,
                    kind="slack",
                    adapter=CLUSTER_MESSAGE_ADAPTER,
                    conversation_id=conversation,
                )
                async with httpx.AsyncClient() as transport:
                    reader = ApprovalClient(
                        api_base_url=config.api_base_url,
                        api_key=get_settings().api_key,
                        client=transport,
                        read_timeout_s=5,
                    )
                    for _ in range(513):
                        await deliver_reminder(
                            ApprovalReminderV1(
                                version=1,
                                approval_id=approval_id,
                                claimed_due_at=datetime.now(UTC),
                            ),
                            reader=reader,
                            cards=cards,
                            sink=relay,
                        )
                assert await client.llen(keys[0]) == 1
                assert 0 < await client.pttl(keys[0]) <= 600_000
                event["message"]["text"] = "Resumed answer"
                await relay.emit(ReplyPost.model_validate(event), route=route)
                page = case.client.get(
                    f"/cluster-message-replies/{relay_ref}",
                    headers={"X-API-Key": get_settings().api_key},
                )
                assert page.status_code == 200, page.text
                assert [entry["message"]["text"] for entry in page.json()["events"]] == [
                    "Original card",
                    "Resumed answer",
                ]
                await client.delete(*keys)
                await cards.remember(
                    approval_id,
                    channel=INBOX,
                    ts=ack["ref"],
                    summary="Send the quote",
                    endpoint=endpoint,
                    kind="email",
                    adapter=MAIL_ADAPTER,
                    conversation_id=conversation,
                )
                async with httpx.AsyncClient() as transport:
                    await deliver_reminder(
                        ApprovalReminderV1(
                            version=1,
                            approval_id=approval_id,
                            claimed_due_at=datetime.now(UTC),
                        ),
                        reader=ApprovalClient(
                            api_base_url=config.api_base_url,
                            api_key=get_settings().api_key,
                            client=transport,
                            read_timeout_s=5,
                        ),
                        cards=cards,
                        sink=sink,
                    )
            finally:
                await relay.aclose()
                await sink.aclose()
                await client.aclose()

        asyncio.run(notify())
        reminder = mail.replies_to("msg-original")
        assert reminder == [original[0], "Awaiting your decision: Send the quote"]
        assert mail.deliveries[-1][1] == frozenset({requester, SENDER})
        assert APPROVAL_REF_PATTERN.findall(reminder[-1]) == []
        assert len(adapter.state.approval_refs_in(conversation)) == 1
        assert adapter.state.approval_ref_for(approval_id)["reference"] == reference
        assert adapter.state.live_reply_refs(conversation) == []
        assert len(case.provider.requests) == before_lookups
        assert valkey.xrange(runs_stream) == before_stream

        def reply(message_id: str, quoted: str = "") -> None:
            mail.add_inbound(
                message_id,
                conversation,
                sender=SENDER,
                text="APPROVE",
                full_text=f"APPROVE\n\n{quoted}",
                headers={"From": SENDER, "Message-ID": f"<{message_id}@example.com>"},
                to=[INBOX],
            )
            mail.verdicts[message_id] = verifying.verified("example.com")
            adapter.poll_once()

        reply("missing-reference")
        assert APPROVAL_INSTRUCTIONS in mail.replies_to("missing-reference")[0]
        assert answers == []
        assert rows(case.row["dedupe_key"])[0]["status"] == "pending"
        assert valkey.xrange(runs_stream) == before_stream
        assert resolve(case, *email_answer(case, key=b"f" * 32)).status_code == 401
        reply("answer", f"> Approval reference: {reference}")
        assert answers[-1][0] == (200 if eligible else 403), answers
        row = rows(case.row["dedupe_key"])[0]
        assert adapter.state.approval_ref_for(approval_id)["reference"] == reference
        assert len(adapter.state.approval_refs_in(conversation)) == 1
        added = valkey.xrange(runs_stream)[len(before_stream) :]
        if eligible:
            assert row["status"] == "approved" and row["resolved_by"] == SENDER, answers
            assert len(added) == 1
            valkey.xdel(runs_stream, added[0][0])
        else:
            assert row["status"] == "pending" and row["resolved_by"] is None
            assert added == []
    finally:
        adapter.shutdown.set()
        adapter.close()
        for server in (egress, api_server, provider_server):
            server.shutdown()
            server.server_close()
        egress_thread.join()
        api_thread.join()
