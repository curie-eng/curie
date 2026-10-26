"""Curie email channel adapter.

Polls one mailbox through a mail provider (ADR 0228; AgentMail is built in) and
POSTs each admitted message to the platform's channel ingress under a scoped
channel token, and serves the neutral reply wire, sending one threaded reply per
`turn.completed`. Only a provider that verifies senders can open inbound mail;
AgentMail cannot, so an AgentMail install refuses every message. It holds no
platform API key, no queue credential, and no platform database access. Its
local SQLite file holds delivery and reply ownership on the chart's one-writer
RWO volume; binding is an operator action at deploy time.

Full behavior spec in `apps/mail-adapter/README.md`.
"""

from .adapter import EVENT_MARKER, MailAdapter
from .agentmail import AgentMailClient, AgentMailProvider
from .config import MailAdapterConfig
from .egress import ADAPTER_SECRET_HEADER, make_server
from .provider import INTERFACE_VERSION, MailProvider

__version__ = "0.0.0"

__all__ = [
    "ADAPTER_SECRET_HEADER",
    "EVENT_MARKER",
    "INTERFACE_VERSION",
    "AgentMailClient",
    "AgentMailProvider",
    "MailAdapter",
    "MailAdapterConfig",
    "MailProvider",
    "__version__",
    "make_server",
]
