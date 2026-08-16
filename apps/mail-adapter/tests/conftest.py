"""Fixtures wiring the two fake external services to a real `MailAdapter`.

The fakes themselves live in `_support.py` (see its docstring for the mocking
rule and the provider behavior they reproduce); this file only starts them, puts
this directory on `sys.path` so test modules can import `_support`, and builds a
fresh adapter instance per test. There is no state-clearing fixture: a new
instance per test is strictly stronger isolation than resetting globals, which
is one of the reasons the spike was promoted.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable, Iterator
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.agentmail import AgentMailClient
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.egress import make_server

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _support import (  # noqa: E402
    AGENTMAIL_API_KEY,
    ALLOWED_SENDER,
    CHANNEL_TOKEN,
    EGRESS_SECRET,
    INBOX,
    IngressHandler,
    IngressState,
    MailHandler,
    MailState,
    RedirectHandler,
    RedirectState,
    SinkHandler,
    SinkState,
    serve,
)


@pytest.fixture
def mail() -> Iterator[MailState]:
    state = MailState()
    server = serve(MailHandler, state)
    state.base_url = f"http://127.0.0.1:{server.server_address[1]}/v0"
    yield state
    state.release_replies()
    server.shutdown()
    server.server_close()


@pytest.fixture
def ingress() -> Iterator[IngressState]:
    state = IngressState()
    server = serve(IngressHandler, state)
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def sink() -> Iterator[SinkState]:
    """The origin a redirect names, recording every header that reaches it."""
    state = SinkState()
    server = serve(SinkHandler, state)
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def redirect(sink: SinkState) -> Iterator[RedirectState]:
    """An origin that answers every request with a 3xx pointing at `sink`."""
    state = RedirectState()
    server = serve(RedirectHandler, state)
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    state.location = sink.url + "/"
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def make_config(mail: MailState, ingress: IngressState) -> Callable[..., MailAdapterConfig]:
    """A config pointed at the two fake servers. Overrides are field-name kwargs."""

    def _make(**overrides: Any) -> MailAdapterConfig:
        kwargs: dict[str, Any] = {
            "agentmail_api_key": AGENTMAIL_API_KEY,
            "agentmail_inbox": INBOX,
            "agentmail_base_url": mail.base_url,
            "api_base_url": ingress.url,
            "channel_token": CHANNEL_TOKEN,
            "egress_secret": EGRESS_SECRET,
            "ingress_enabled": True,
            "poll_interval_seconds": 0.05,
            "ingress_retry_delay_seconds": 0.01,
            "port": 0,
            "allowed_senders": (ALLOWED_SENDER,),
        }
        kwargs.update(overrides)
        return MailAdapterConfig(**kwargs)

    return _make


@pytest.fixture
def make_adapter(make_config: Callable[..., MailAdapterConfig]) -> Callable[..., MailAdapter]:
    def _make(*, client: AgentMailClient | None = None, **overrides: Any) -> MailAdapter:
        return MailAdapter(make_config(**overrides), client=client)

    return _make


@pytest.fixture
def adapter(make_adapter: Callable[..., MailAdapter]) -> Iterator[MailAdapter]:
    instance = make_adapter()
    yield instance
    instance.shutdown.set()


@pytest.fixture
def serve_egress() -> Iterator[Callable[[MailAdapter], str]]:
    """Start the adapter's OWN egress server, exactly as the platform reaches it."""
    servers: list[ThreadingHTTPServer] = []

    def _serve_one(instance: MailAdapter) -> str:
        server = make_server(instance, 0)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield _serve_one
    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture
def egress_url(adapter: MailAdapter, serve_egress: Callable[[MailAdapter], str]) -> str:
    return serve_egress(adapter) + "/"
