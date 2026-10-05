"""The channel adapters the conformance kit runs against, one registry entry each.

Every subject is driven through the worker's REAL egress seam: a
``ReplySinkRouter`` with ``GitHubReplySink`` under ``github`` and an
``HttpReplyAdapter`` as the default, addressed by a ``TargetRoute``. That is the
kernel's whole view of egress, so each check measures what the platform would
actually see, and a header-name or status mismatch between the worker and an
adapter (the Discord ``X-Curie-Adapter-Key`` defect) fails here by construction
rather than passing two unit suites that each assert their own side. A
best-effort emit goes through the same router with ``best_effort_unreachable``
set, so how the worker CLASSIFIES an adapter's failure is measured too.

Only the EXTERNAL seams are faked: the Discord API (``DiscordPort``), AgentMail
and the platform ingress (the mail suite's own fakes), and a relay's third-party
far end. Everything inside Curie code runs for real over loopback HTTP. The
fakes outlive ``restart()``, as a provider outlives the adapter's pod.

Every resource is entered on an ``AsyncExitStack`` as soon as it exists and
before anything is awaited, so a startup that fails half way still releases
what it already holds.

Adding an adapter is one ``_Entry`` in ``_ENTRIES``: its declared capabilities,
which decide the checks it gets, and a factory that opens it.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import itertools
import json
import threading
import uuid
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import uvicorn
from _support import (
    AGENTMAIL_API_KEY,
    ALLOWED_SENDER,
    CHANNEL_TOKEN,
    EGRESS_SECRET,
    INBOX,
    IngressHandler,
    IngressState,
    MailHandler,
    MailState,
    seed_historical_reply,
    serve,
)
from aiohttp import web
from channel_protocol import ReplyAck, ReplyEvent
from channel_protocol.conformance import (
    Capabilities,
    ChannelAdapterSubject,
    Effect,
    IngressTurn,
    Streaming,
    Upstream,
)
from curie_api.github_factory_events import parse_factory_event
from curie_api.workitems.lifecycle import github_reply_route
from curie_discord_adapter.config import DiscordConfig
from curie_discord_adapter.egress import DiscordReplyService
from curie_discord_adapter.http import create_reply_app
from curie_discord_adapter.ingress import DiscordBinding
from curie_discord_adapter.main import DiscordAdapter
from curie_discord_adapter.state import DiscordState
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.egress import EgressHandler, EgressServer
from curie_worker.reply_sink import (
    GitHubReplySink,
    HttpReplyAdapter,
    ReplySinkRouter,
    TargetRoute,
)

SubjectFactory = Callable[[Path], contextlib.AbstractAsyncContextManager[ChannelAdapterSubject]]

# The header name as the channel-adapter guide documents it, deliberately NOT
# imported from the worker: a far end that borrowed the worker's constant would
# agree with any rename by construction, which is the exact way the Discord
# header mismatch hid in two green suites.
_DOCUMENTED_SECRET_HEADER = "X-Curie-Adapter-Secret"
_LOOPBACK = "127.0.0.1"
_STARTUP_TIMEOUT_S = 10.0


class _KernelEgress:
    """The worker's egress seam for one binding, plus a forged twin.

    ``forged`` is the same router shape holding a WRONG secret for the same
    adapter slug, so ``emit_unauthenticated`` reaches the adapter over the exact
    transport a real delivery uses and differs only in the credential.
    ``endpoint`` is reassignable because a restarted adapter listens anew.
    """

    def __init__(self, *, endpoint: str | None, slug: str | None, secret: str | None) -> None:
        self._slug = slug
        self.route = TargetRoute(endpoint=endpoint, adapter=slug)
        credentials = {slug: secret} if slug and secret else {}
        forged = {slug: f"not-{secret}"} if slug and secret else {}
        self._router = ReplySinkRouter(
            adapters={"github": GitHubReplySink()}, default=HttpReplyAdapter(credentials)
        )
        self._forged = ReplySinkRouter(
            adapters={"github": GitHubReplySink()}, default=HttpReplyAdapter(forged)
        )

    def point_at(self, endpoint: str) -> None:
        self.route = TargetRoute(endpoint=endpoint, adapter=self._slug)

    async def emit(self, event: ReplyEvent, *, best_effort: bool = False) -> ReplyAck:
        return await self._router.emit(event, route=self.route, best_effort_unreachable=best_effort)

    async def emit_forged(self, event: ReplyEvent) -> ReplyAck:
        return await self._forged.emit(event, route=self.route)

    async def aclose(self) -> None:
        await self._router.aclose()
        await self._forged.aclose()


@contextlib.contextmanager
def _threaded(server: ThreadingHTTPServer) -> Iterator[int]:
    """Serve on a daemon thread; the port is already bound by construction."""

    try:
        threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
        ).start()
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()


@contextlib.asynccontextmanager
async def _uvicorn(app: Any) -> AsyncIterator[int]:
    """Serve ``app`` on loopback port 0 inside the running loop.

    The serve task is cancelled if startup never completes, and always awaited,
    so a failed start leaves no task or socket behind.
    """

    server = uvicorn.Server(
        uvicorn.Config(app, host=_LOOPBACK, port=0, log_config=None, lifespan="off")
    )
    task = asyncio.create_task(server.serve())
    try:
        deadline = asyncio.get_running_loop().time() + _STARTUP_TIMEOUT_S
        while not server.started:
            if task.done():
                raise RuntimeError("uvicorn exited before it started")
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError("uvicorn did not start")
            await asyncio.sleep(0.01)
        yield int(server.servers[0].sockets[0].getsockname()[1])
    finally:
        server.should_exit = True
        if not server.started:
            task.cancel()
        await asyncio.wait({task})
        if not task.cancelled():
            task.exception()  # retrieved: a failed start is reported above


class _KernelEgressSubject:
    """A subject whose reply events go through its ``_KernelEgress``."""

    _egress: _KernelEgress

    async def emit(self, event: ReplyEvent, *, best_effort: bool = False) -> ReplyAck:
        return await self._egress.emit(event, best_effort=best_effort)

    async def emit_unauthenticated(self, event: ReplyEvent) -> ReplyAck:
        return await self._egress.emit_forged(event)


# --- discord ----------------------------------------------------------------

DISCORD_CAPABILITIES = Capabilities(
    kind="discord",
    streaming=Streaming.EDIT,
    ingress=True,
    authenticates=True,
    refuses_foreign_targets=True,
    serves_attachments=False,
    settles_approval_cards=True,
)
_DISCORD_BOT_ID = "900000000000000001"
_DISCORD_PARENT_CHANNEL = "800000000000000001"
_DISCORD_SECRET = "discord-conformance-secret"
_DISCORD_CHANNEL_TOKEN = "chn-discord-conformance"
_DISCORD_SLUG = "discord-conformance"


class DiscordProviderError(RuntimeError):
    """The fake Discord API refused one call, as a 5xx or a rate limit would."""


class FakeDiscordPort:
    """The Discord REST API, reduced to the three calls ``DiscordPort`` makes.

    It keeps the visible text of every message so an edit of a message that was
    never posted is refused the way Discord answers Unknown Message, and records
    each post, edit and delete as an ``Effect``. Placeholders the ingress side
    posts are seeded directly and are not effects: they belong to ingress.

    ``fail_next`` fails the next call once. With ``accepted`` the call takes
    effect first and THEN fails, the way a response lost after Discord applied
    the request looks to the adapter.
    """

    def __init__(self) -> None:
        self.messages: dict[str, str] = {}
        self.effects: list[Effect] = []
        self.fail_next = False
        self.accepted = False
        self._ids = itertools.count(700000000000000001)

    def seed_placeholder(self, text: str) -> str:
        message_id = str(next(self._ids))
        self.messages[message_id] = text
        return message_id

    def _record(self, effect: Effect) -> None:
        failing, self.fail_next = self.fail_next, False
        if failing and not self.accepted:
            raise DiscordProviderError("injected Discord API failure")
        self.effects.append(effect)
        if failing:
            raise DiscordProviderError("injected Discord API failure after the effect")

    async def post_message(self, channel_id: str, text: str) -> str:
        del channel_id
        message_id = str(next(self._ids))
        self._record(Effect(op="post", ref=message_id, text=text))
        self.messages[message_id] = text
        return message_id

    async def edit_message(self, channel_id: str, message_id: str, text: str) -> None:
        del channel_id
        if message_id not in self.messages:
            raise DiscordProviderError(f"Unknown Message {message_id}")
        self._record(Effect(op="edit", ref=message_id, text=text))
        self.messages[message_id] = text

    async def delete_message(self, channel_id: str, message_id: str) -> None:
        del channel_id
        self._record(Effect(op="delete", ref=message_id, text=""))
        self.messages.pop(message_id, None)


class _DiscordIngressState:
    def __init__(self, token: str) -> None:
        self.token = token
        self.bodies: list[dict[str, Any]] = []
        self.drop_next = 0


class _DroppingIngressHandler(BaseHTTPRequestHandler):
    """The platform's ``POST /channels/turns``, able to drop one connection.

    The body is read and recorded BEFORE a drop, so a dropped attempt and its
    retry are both visible: the stability check compares exactly those two.
    """

    protocol_version = "HTTP/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        state = cast(_DiscordIngressState, self.server.state)  # type: ignore[attr-defined]
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        state.bodies.append(body)
        if state.drop_next > 0:
            state.drop_next -= 1
            self.close_connection = True  # a transport failure at the adapter
            return
        status = 200
        payload: dict[str, Any] = {
            "event_id": f"chn-{uuid.uuid4().hex}",
            "stream_id": "1-0",
            "duplicate": False,
        }
        if self.headers.get("X-API-Key") != state.token:
            status, payload = 401, {"detail": "missing or invalid credential"}
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


@dataclass(frozen=True)
class _FakeUser:
    """discord.py's ``User``, as far as ``on_message`` reads it."""

    id: int
    display_name: str
    bot: bool = False


_DISCORD_BOT_USER = _FakeUser(id=int(_DISCORD_BOT_ID), display_name="curie", bot=True)


@dataclass(frozen=True)
class _FakeAttachment:
    id: int
    filename: str
    size: int


@dataclass
class _FakePlaceholder:
    """A message the bot posted; its id is the turn's reply_ref."""

    id: int

    async def edit(self, *, content: str, **kwargs: Any) -> None:
        del kwargs
        raise AssertionError(f"ingress failed and rewrote the placeholder: {content!r}")


@dataclass
class _FakeThread:
    id: int
    port: FakeDiscordPort

    async def send(self, content: str, **kwargs: Any) -> _FakePlaceholder:
        del kwargs
        # The placeholder lives in the fake provider, so the reply wire can edit
        # it, but it is ingress's doing and is not recorded as an effect.
        return _FakePlaceholder(id=int(self.port.seed_placeholder(content)))

    async def delete(self) -> None:
        return None


@dataclass
class _FakeChannel:
    """A bound parent text channel: deliberately NOT a ``discord.Thread``."""

    id: int
    port: FakeDiscordPort
    thread_ids: Iterator[int]


@dataclass
class _FakeGatewayMessage:
    """discord.py's ``Message`` at the SDK boundary, duck-typed."""

    id: str
    author: _FakeUser
    channel: _FakeChannel
    content: str
    mentions: list[_FakeUser]
    attachments: list[_FakeAttachment]

    async def create_thread(self, *, name: str) -> _FakeThread:
        del name
        return _FakeThread(id=next(self.channel.thread_ids), port=self.channel.port)


class DiscordSubject(_KernelEgressSubject):
    """The real reply app and the real Gateway client's ingress post.

    One "generation" is the adapter process: its SQLite state, its Gateway
    client and its reply server. ``restart`` ends one and starts the next on
    the same state file, the way a replacement pod would.
    """

    capabilities = DISCORD_CAPABILITIES

    def __init__(
        self,
        *,
        tmp: Path,
        port: FakeDiscordPort,
        ingress: _DiscordIngressState,
        ingress_url: str,
        egress: _KernelEgress,
    ) -> None:
        self._state_path = tmp / "discord-state.sqlite3"
        self._port = port
        self._ingress = ingress
        self._egress = egress
        binding = DiscordBinding(
            parent_channel_id=_DISCORD_PARENT_CHANNEL,
            address=_DISCORD_PARENT_CHANNEL,
            token=_DISCORD_CHANNEL_TOKEN,
        )
        self._config = DiscordConfig(
            discord_bot_token="conformance-bot-token",
            adapter_secret=_DISCORD_SECRET,
            curie_api_url=ingress_url,
            bindings=[binding],
            bindings_path=None,
            state_path=self._state_path,
            placeholder_text="On it. Working on your request.",
        )
        self._threads = itertools.count(600000000000000001)
        self._generation: contextlib.AsyncExitStack | None = None
        self._adapter: DiscordAdapter | None = None

    async def start(self) -> None:
        stack = contextlib.AsyncExitStack()
        self._generation = stack
        state = DiscordState(self._state_path)
        stack.callback(state.close)
        adapter = DiscordAdapter(self._config, state)
        stack.push_async_callback(adapter.close)
        # What the Gateway READY event sets: the client's own user, which
        # ``Client.user`` reads from the connection state. No method is patched.
        adapter._connection.user = _DISCORD_BOT_USER  # type: ignore[assignment]
        self._adapter = adapter
        reply_port = await stack.enter_async_context(
            _uvicorn(create_reply_app(DiscordReplyService(self._port, state), _DISCORD_SECRET))
        )
        self._egress.point_at(f"http://{_LOOPBACK}:{reply_port}/replies")

    async def stop(self) -> None:
        generation, self._generation = self._generation, None
        if generation is not None:
            await generation.aclose()

    async def restart(self) -> None:
        await self.stop()
        await self.start()

    async def open_conversation(self, message: Upstream) -> list[IngressTurn]:
        # The REAL Gateway handler, handed a message shaped like discord.py's.
        # It opens a thread, posts the placeholder whose id becomes reply_ref,
        # and posts the turn through ``_post_turn`` to the fake ingress, which
        # drops the first connection so the transport retry is captured too.
        # The upstream files ride on ``message.attachments``; the adapter
        # declares it serves none, so they must not reach the turn.
        assert self._adapter is not None
        mark = len(self._ingress.bodies)
        self._ingress.drop_next = 1
        await self._adapter.on_message(
            _FakeGatewayMessage(  # type: ignore[arg-type]
                id=message.id,
                author=_FakeUser(id=500000000000000001, display_name="conformance-user"),
                channel=_FakeChannel(
                    id=int(_DISCORD_PARENT_CHANNEL),
                    port=self._port,
                    thread_ids=self._threads,
                ),
                content=f"<@{_DISCORD_BOT_ID}> {message.text}",
                mentions=[_DISCORD_BOT_USER],
                attachments=[
                    _FakeAttachment(id=index, filename=item.name, size=len(item.content))
                    for index, item in enumerate(message.attachments, start=1)
                ],
            )
        )
        return [_turn_from_body(body) for body in self._ingress.bodies[mark:]]

    async def fetch_attachment(self, attachment_id: str) -> bytes:
        raise AssertionError(
            f"Discord declares serves_attachments=False; nothing may fetch {attachment_id!r}"
        )

    def fail_next_delivery(self, *, accepted: bool = False) -> None:
        self._port.fail_next = True
        self._port.accepted = accepted

    def effects(self) -> Sequence[Effect]:
        return list(self._port.effects)


@contextlib.asynccontextmanager
async def open_discord(tmp: Path) -> AsyncIterator[ChannelAdapterSubject]:
    async with contextlib.AsyncExitStack() as stack:
        port = FakeDiscordPort()
        ingress = _DiscordIngressState(_DISCORD_CHANNEL_TOKEN)
        ingress_server = serve(_DroppingIngressHandler, ingress)
        stack.callback(ingress_server.server_close)
        stack.callback(ingress_server.shutdown)
        ingress_port = ingress_server.server_address[1]
        egress = _KernelEgress(endpoint=None, slug=_DISCORD_SLUG, secret=_DISCORD_SECRET)
        stack.push_async_callback(egress.aclose)
        subject = DiscordSubject(
            tmp=tmp,
            port=port,
            ingress=ingress,
            ingress_url=f"http://{_LOOPBACK}:{ingress_port}",
            egress=egress,
        )
        stack.push_async_callback(subject.stop)
        await subject.start()
        yield subject


# --- mail -------------------------------------------------------------------

MAIL_CAPABILITIES = Capabilities(
    kind="email",
    streaming=Streaming.BUFFERED,
    # Current AgentMail intake cannot verify a positive authentication verdict.
    ingress=False,
    authenticates=True,
    refuses_foreign_targets=True,
    serves_attachments=False,
    settles_approval_cards=True,
)
_MAIL_SLUG = "mail-conformance"
# ADR-0177's adapter principal. Set, the adapter renders an approval card as an
# answerable email with its own card ref and sends one follow-up per settlement.
_MAIL_PRINCIPAL = "adp-conformance-principal"


class MailSubject(_KernelEgressSubject):
    """The real ``MailAdapter``, its own egress server, and the mail suite's fakes.

    One generation is the adapter process and its egress server; ``restart``
    closes it and opens a replacement on the same SQLite state, exactly as
    ``restarted_adapter`` in the mail suite does.

    Egress targets come from historical accepted turns. Fresh provider mail
    remains subject to the real adapter's authentication refusal.
    """

    capabilities = MAIL_CAPABILITIES

    def __init__(
        self,
        *,
        config: MailAdapterConfig,
        mail: MailState,
        ingress: IngressState,
        egress: _KernelEgress,
    ) -> None:
        self._config = config
        self._mail = mail
        self._ingress = ingress
        self._egress = egress
        self._generation: contextlib.AsyncExitStack | None = None
        self._adapter: MailAdapter | None = None

    async def start(self) -> None:
        stack = contextlib.AsyncExitStack()
        self._generation = stack
        adapter = MailAdapter(self._config)
        stack.callback(adapter.close)
        stack.callback(adapter.shutdown.set)
        self._adapter = adapter
        # ``EgressServer`` + ``EgressHandler`` is exactly what ``make_server``
        # builds; constructed directly only to bind loopback, not ``0.0.0.0``.
        server = EgressServer((_LOOPBACK, 0), EgressHandler, adapter)
        server.daemon_threads = True
        egress_port = stack.enter_context(_threaded(server))
        self._egress.point_at(f"http://{_LOOPBACK}:{egress_port}/")

    async def stop(self) -> None:
        generation, self._generation = self._generation, None
        if generation is not None:
            await generation.aclose()

    async def restart(self) -> None:
        await self.stop()
        await self.start()

    async def open_conversation(self, message: Upstream) -> list[IngressTurn]:
        """Prepare an egress target from a turn accepted before the authentication gate."""
        assert self._adapter is not None
        conversation_id = f"thr-{message.id}"
        seed_historical_reply(
            self._mail,
            self._adapter.state,
            message.id,
            conversation_id,
            sender=ALLOWED_SENDER,
            subject="Conformance",
            text=message.text,
        )
        # The non-ingress subject returns a stand-in, never a posted channel turn.
        return [
            IngressTurn(
                kind=MAIL_CAPABILITIES.kind,
                address=INBOX,
                delivery_id=message.id,
                conversation_id=conversation_id,
                reply_ref=message.id,
                body=None,
            )
        ]

    async def fetch_attachment(self, attachment_id: str) -> bytes:
        raise AssertionError(
            f"mail declares serves_attachments=False; nothing may fetch {attachment_id!r}"
        )

    def fail_next_delivery(self, *, accepted: bool = False) -> None:
        if accepted:
            # AgentMail sends and threads the reply, then the response is lost.
            self._mail.accept_then_drop_next_reply = True
        else:
            self._mail.fail_next_reply = 503

    def effects(self) -> Sequence[Effect]:
        return [
            Effect(op="send", ref=message_id, text=text) for message_id, text in self._mail.replies
        ]


@contextlib.asynccontextmanager
async def open_mail(tmp: Path) -> AsyncIterator[ChannelAdapterSubject]:
    async with contextlib.AsyncExitStack() as stack:
        mail = MailState()
        mail_server = serve(MailHandler, mail)
        stack.callback(mail_server.server_close)
        stack.callback(mail_server.shutdown)
        stack.callback(mail.release_replies)
        mail.base_url = f"http://{_LOOPBACK}:{mail_server.server_address[1]}/v0"
        ingress = IngressState()
        ingress_server = serve(IngressHandler, ingress)
        stack.callback(ingress_server.server_close)
        stack.callback(ingress_server.shutdown)
        ingress.url = f"http://{_LOOPBACK}:{ingress_server.server_address[1]}"
        config = MailAdapterConfig(
            agentmail_api_key=AGENTMAIL_API_KEY,
            agentmail_inbox=INBOX,
            agentmail_base_url=mail.base_url,
            api_base_url=ingress.url,
            channel_token=CHANNEL_TOKEN,
            egress_secret=EGRESS_SECRET,
            adapter_principal=_MAIL_PRINCIPAL,
            ingress_enabled=True,
            poll_interval_seconds=0.05,
            ingress_retry_delay_seconds=0.01,
            port=0,
            allowed_senders=(ALLOWED_SENDER,),
            state_path=str(tmp / "mail-state.sqlite3"),
        )
        egress = _KernelEgress(endpoint=None, slug=_MAIL_SLUG, secret=EGRESS_SECRET)
        stack.push_async_callback(egress.aclose)
        subject = MailSubject(config=config, mail=mail, ingress=ingress, egress=egress)
        stack.push_async_callback(subject.stop)
        await subject.start()
        yield subject


# --- github -----------------------------------------------------------------

GITHUB_CAPABILITIES = Capabilities(
    kind="github",
    streaming=Streaming.SILENT,
    ingress=True,
    authenticates=False,
    refuses_foreign_targets=False,
    serves_attachments=False,
    settles_approval_cards=False,
)
_GITHUB_REPO = "acme-corp/acme-bot"
_GITHUB_MENTION = "curie"
_GITHUB_SENDER = {"id": 6601, "login": "octocat", "type": "User"}


class GitHubSubject(_KernelEgressSubject):
    """Factory mention intake on the ingress side, ``GitHubReplySink`` on egress.

    The platform's one GitHub writer is the factory notice path, so the sink
    acks and posts nothing; ``effects`` is empty because no provider call
    exists to record, which is the behavior the SILENT mode pins. Both sides
    are stateless, so ``restart`` has nothing to reopen.
    """

    capabilities = GITHUB_CAPABILITIES

    def __init__(self, egress: _KernelEgress) -> None:
        self._egress = egress
        self._comment_ids: dict[str, int] = {}
        self._issue_numbers: dict[str, int] = {}
        self._next_comment = itertools.count(3_000_001)
        self._next_issue = itertools.count(9_100)

    def _payload(self, message: Upstream) -> dict[str, Any]:
        if message.id not in self._comment_ids:
            self._comment_ids[message.id] = next(self._next_comment)
            self._issue_numbers[message.id] = next(self._next_issue)
        return {
            "action": "created",
            "installation": {"id": 5501},
            "repository": {"id": 4401, "full_name": _GITHUB_REPO},
            "sender": dict(_GITHUB_SENDER),
            "issue": {"number": self._issue_numbers[message.id], "state": "open"},
            "comment": {
                "id": self._comment_ids[message.id],
                "body": f"@{_GITHUB_MENTION} {message.text}",
                "user": dict(_GITHUB_SENDER),
                "performed_via_github_app": None,
            },
        }

    async def open_conversation(self, message: Upstream) -> list[IngressTurn]:
        # GitHub redelivers a webhook under a NEW X-GitHub-Delivery id, so the
        # same comment arrives twice with two delivery UUIDs.
        payload = self._payload(message)
        turns: list[IngressTurn] = []
        for _ in range(2):
            notice = parse_factory_event(
                "issue_comment",
                payload,
                str(uuid.uuid4()),
                label="factory",
                mention=_GITHUB_MENTION,
            )
            kind, address, conversation_id = github_reply_route(
                notice.repo_full_name, notice.issue_number
            )
            turns.append(
                IngressTurn(
                    kind=kind,
                    address=address,
                    delivery_id=str(notice.request_id),
                    conversation_id=conversation_id,
                    reply_ref=None,
                    body=None,
                )
            )
        return turns

    async def emit_unauthenticated(self, event: ReplyEvent) -> ReplyAck:
        # The GitHub sink holds no egress credential, so there is none to forge.
        return await self._egress.emit(event)

    async def fetch_attachment(self, attachment_id: str) -> bytes:
        raise AssertionError(
            f"GitHub declares serves_attachments=False; nothing may fetch {attachment_id!r}"
        )

    async def restart(self) -> None:
        return None

    def fail_next_delivery(self, *, accepted: bool = False) -> None:
        raise AssertionError(
            "the GitHub reply sink makes no provider call, so there is nothing to fail "
            f"(accepted={accepted}); a SILENT subject must never be handed a failure check"
        )

    def effects(self) -> Sequence[Effect]:
        return []


@contextlib.asynccontextmanager
async def open_github(tmp: Path) -> AsyncIterator[ChannelAdapterSubject]:
    del tmp
    async with contextlib.AsyncExitStack() as stack:
        egress = _KernelEgress(endpoint=None, slug=None, secret=None)
        stack.push_async_callback(egress.aclose)
        yield GitHubSubject(egress)


# --- http-reply -------------------------------------------------------------

HTTP_REPLY_CAPABILITIES = Capabilities(
    kind="conformance-relay",
    streaming=Streaming.RELAY,
    ingress=False,
    authenticates=True,
    refuses_foreign_targets=False,
    serves_attachments=False,
    settles_approval_cards=False,
)
_RELAY_SECRET = "relay-conformance-secret"
_RELAY_REF = "relay-ref-1"


class _RelayFarEnd:
    """A third-party HTTP endpoint bound to the generic relay.

    It verifies the documented secret header (401 otherwise), records each body
    verbatim as a ``forward`` effect, and acks with one stable ``ref``. An armed
    failure answers 503 once; with ``accepted`` the body is recorded first, so
    the far end acted and only its answer failed.
    """

    def __init__(self, secret: str) -> None:
        self._secret = secret
        self.effects: list[Effect] = []
        self.fail_next = False
        self.accepted = False

    async def handle(self, request: web.Request) -> web.Response:
        presented = request.headers.get(_DOCUMENTED_SECRET_HEADER, "")
        if not hmac.compare_digest(presented.encode(), self._secret.encode()):
            return web.json_response({"detail": "missing or invalid credential"}, status=401)
        raw = await request.text()
        failing, self.fail_next = self.fail_next, False
        if failing and not self.accepted:
            return web.json_response({"detail": "injected far-end failure"}, status=503)
        self.effects.append(Effect(op="forward", ref=None, text=raw))
        if failing:
            return web.json_response({"detail": "injected failure after acting"}, status=503)
        return web.json_response({"ref": _RELAY_REF})


class HttpReplySubject(_KernelEgressSubject):
    """``HttpReplyAdapter`` relaying to a far end that owns no ingress or state."""

    capabilities = HTTP_REPLY_CAPABILITIES

    def __init__(self, far_end: _RelayFarEnd, egress: _KernelEgress) -> None:
        self._far_end = far_end
        self._egress = egress

    async def open_conversation(self, message: Upstream) -> list[IngressTurn]:
        # No ingress: one stand-in turn so the checks can address the far end.
        return [
            IngressTurn(
                kind=HTTP_REPLY_CAPABILITIES.kind,
                address="relay-address",
                delivery_id=f"relay-{message.id}",
                conversation_id=f"relay-conversation-{message.id}",
                reply_ref=None,
                body=None,
            )
        ]

    async def fetch_attachment(self, attachment_id: str) -> bytes:
        raise AssertionError(
            f"the relay declares serves_attachments=False; nothing may fetch {attachment_id!r}"
        )

    async def restart(self) -> None:
        return None

    def fail_next_delivery(self, *, accepted: bool = False) -> None:
        self._far_end.fail_next = True
        self._far_end.accepted = accepted

    def effects(self) -> Sequence[Effect]:
        return list(self._far_end.effects)


@contextlib.asynccontextmanager
async def open_http_reply(tmp: Path) -> AsyncIterator[ChannelAdapterSubject]:
    del tmp
    async with contextlib.AsyncExitStack() as stack:
        far_end = _RelayFarEnd(_RELAY_SECRET)
        egress = _KernelEgress(endpoint=None, slug="relay-conformance", secret=_RELAY_SECRET)
        stack.push_async_callback(egress.aclose)
        app = web.Application()
        app.router.add_post("/relay", far_end.handle)
        runner = web.AppRunner(app, access_log=None)
        # Registered before setup: ``cleanup`` is safe on a runner that never
        # finished setting up, and it stops every site the runner registered,
        # so a setup or bind failure below releases what it already holds.
        stack.push_async_callback(runner.cleanup)
        await runner.setup()
        await web.TCPSite(runner, _LOOPBACK, 0).start()
        egress.point_at(f"http://{_LOOPBACK}:{int(runner.addresses[0][1])}/relay")
        yield HttpReplySubject(far_end, egress)


# --- shared -----------------------------------------------------------------


def _turn_from_body(body: dict[str, Any]) -> IngressTurn:
    """The ``IngressTurn`` an adapter's exact ``POST /channels/turns`` body names."""

    return IngressTurn(
        kind=body["kind"],
        address=body["address"],
        delivery_id=body["delivery_id"],
        conversation_id=body["conversation_id"],
        reply_ref=body.get("reply_ref"),
        attachments=tuple(body.get("attachments") or ()),
        body=body,
    )


@dataclass(frozen=True)
class _Entry:
    capabilities: Capabilities
    open: SubjectFactory


_ENTRIES: dict[str, _Entry] = {
    "discord": _Entry(DISCORD_CAPABILITIES, open_discord),
    "mail": _Entry(MAIL_CAPABILITIES, open_mail),
    "github": _Entry(GITHUB_CAPABILITIES, open_github),
    "http-reply": _Entry(HTTP_REPLY_CAPABILITIES, open_http_reply),
}

REGISTRY: dict[str, SubjectFactory] = {name: entry.open for name, entry in _ENTRIES.items()}
# Static, so the test matrix is generated at collection time without starting
# a single server.
REGISTRY_CAPABILITIES: dict[str, Capabilities] = {
    name: entry.capabilities for name, entry in _ENTRIES.items()
}
