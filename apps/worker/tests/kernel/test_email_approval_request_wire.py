"""The worker's email approval card, through the real mail adapter (ADR-0177 amendment A5).

The kernel raises an approval on an email turn and posts its card over the
real HTTP reply sink to the real mail adapter's egress server. The adapter
words the request email from the card's ``Approver`` fields and mails it to
the fake AgentMail. Only the two external services are faked: AgentMail and
the platform API. Nothing in the kernel, the sink or the adapter is patched,
so this pins the label both processes agree on by what reaches an inbox.
"""

from __future__ import annotations

import asyncio
import importlib.util
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.egress import make_server
from curie_worker.reply_sink import HttpReplyAdapter

from .test_approval_lifecycle import (
    _REQUESTING_SURFACE,
    RecordingApprovals,
    RoutedBinding,
    _awaiting_routed_script,
    _qevent,
)

_SUPPORT_PATH = Path(__file__).resolve().parents[3] / "mail-adapter/tests/_support.py"
_spec = importlib.util.spec_from_file_location("mail_approval_wire_support", _SUPPORT_PATH)
assert _spec and _spec.loader
support = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(support)

_ADAPTER = "acme-mail"
_APPROVER = "approver@example.com"
_SECOND = "second.approver@example.com"


@contextmanager
def _mail_runtime(tmp_path: Path, *, cc: list[str] | None) -> Iterator[tuple[Any, str]]:
    """A real mail adapter with approvals on, its egress server, and both fakes.

    Seeds one asking turn accepted before upgrade in SQLite, so the adapter
    holds the reply the card is rendered into without admitting new mail.
    """

    mail = support.MailState()
    ingress = support.IngressState()
    provider = support.serve(support.MailHandler, mail)
    api = support.serve(support.IngressHandler, ingress)
    adapter = MailAdapter(
        MailAdapterConfig(
            agentmail_api_key=support.AGENTMAIL_API_KEY,
            agentmail_inbox=support.INBOX,
            agentmail_base_url=f"http://127.0.0.1:{provider.server_port}/v0",
            api_base_url=f"http://127.0.0.1:{api.server_port}",
            channel_token=support.CHANNEL_TOKEN,
            egress_secret=support.EGRESS_SECRET,
            adapter_principal="adp.test-payload.test-signature",
            ingress_enabled=True,
            allowed_senders=(support.ALLOWED_SENDER,),
            state_path=str(tmp_path / "mail.sqlite3"),
        )
    )
    egress = make_server(adapter, 0)
    threading.Thread(target=egress.serve_forever, daemon=True).start()
    try:
        support.seed_historical_reply(
            mail, adapter.state, "msg-ask", "th-wire", text="Please send the quote", cc=cc
        )
        assert adapter.state.live_reply_refs("th-wire") == ["msg-ask"]
        yield mail, f"http://127.0.0.1:{egress.server_port}/"
    finally:
        for server in (egress, api, provider):
            server.shutdown()
            server.server_close()
        adapter.close()


def _request_email(make_harness: Any, tmp_path: Path, cc: list[str] | None) -> tuple[Any, str]:
    """Raise one approval through the kernel and return the inbox and request email."""

    route = {**_REQUESTING_SURFACE, "approvers": {"emails": [_APPROVER, _SECOND.upper()]}}

    async def go() -> tuple[Any, str]:
        with _mail_runtime(tmp_path, cc=cc) as (mail, endpoint):
            sink = HttpReplyAdapter({_ADAPTER: support.EGRESS_SECRET})
            try:
                async with make_harness(
                    approvals=RecordingApprovals(),
                    binding=RoutedBinding({"confirm": route}),
                    sink=sink,
                ) as h:
                    h.runner.default_script = _awaiting_routed_script("Send the quote", "confirm")
                    event = _qevent(
                        "send it",
                        thread="th-wire",
                        kind="email",
                        channel=support.INBOX,
                        endpoint=endpoint,
                        adapter=_ADAPTER,
                        placeholder=None,
                    ).model_copy(update={"author": support.ALLOWED_SENDER})
                    await h.kernel.process_event(event)
            finally:
                await sink.aclose()
            (request_email,) = mail.replies_to("msg-ask")
            return mail, request_email

    return asyncio.run(go())


def test_the_request_email_names_the_routes_approvers_from_the_worker_card(
    make_harness: Any, tmp_path: Path
) -> None:
    mail, request_email = _request_email(make_harness, tmp_path, cc=None)

    assert "Nobody on this thread can approve this request yet." in request_email
    assert f"Only these addresses can approve it: {_APPROVER}, {_SECOND}." in request_email
    assert "add one or more of them, as many as you like" in request_email
    assert mail.received_by(support.ALLOWED_SENDER) == [request_email]
    assert mail.received_by(_APPROVER) == []


def test_a_listed_approver_copied_on_the_ask_receives_the_request_from_the_worker_card(
    make_harness: Any, tmp_path: Path
) -> None:
    mail, request_email = _request_email(make_harness, tmp_path, cc=[_APPROVER])

    assert f"Already on this thread and able to answer: {_APPROVER}." in request_email
    assert mail.received_by(_APPROVER) == [request_email]
    assert mail.received_by(_SECOND) == []
