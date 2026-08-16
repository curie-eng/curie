"""Curie email channel adapter (AgentMail).

Polls an AgentMail inbox and POSTs each new message to the platform's channel
ingress under a scoped channel token, and serves the neutral reply wire, sending
one threaded AgentMail reply per `turn.completed`. It holds no platform API key,
no queue credential, and no database access: binding is an operator action at
deploy time.

Full behavior spec in `apps/mail-adapter/README.md`.
"""

from .adapter import EVENT_MARKER, MailAdapter
from .agentmail import AgentMailClient
from .config import MailAdapterConfig
from .egress import ADAPTER_SECRET_HEADER, make_server

__version__ = "0.0.0"

__all__ = [
    "ADAPTER_SECRET_HEADER",
    "EVENT_MARKER",
    "AgentMailClient",
    "MailAdapter",
    "MailAdapterConfig",
    "__version__",
    "make_server",
]
