"""Curie dispatcher (Slack Bolt, Socket Mode).

Acks Slack events fast, posts an in-thread placeholder, and enqueues a normalized
job onto a Valkey Stream keyed by the delivery's event id (idempotent), under
reconnect supervision. The queue payload is ``aci_protocol.QueuedTurn`` (issue
#7); this package owns its Valkey Stream transport (the ``payload`` encoding and
dedupe) plus the Slack ingress.
"""

from aci_protocol import STREAM_PAYLOAD_FIELD as STREAM_PAYLOAD_FIELD

from .app import (
    SocketModeConnection,
    build_app,
    build_redis,
    build_web_client,
)
from .config import DispatcherConfig
from .handlers import process_event, register_handlers
from .queue import (
    claim_event,
    enqueue,
    from_stream_fields,
    to_stream_fields,
)
from .supervisor import BackoffPolicy, Connection, Supervisor, SupervisorGroup

__version__ = "0.0.0"

__all__ = [
    "STREAM_PAYLOAD_FIELD",
    "BackoffPolicy",
    "Connection",
    "DispatcherConfig",
    "SocketModeConnection",
    "Supervisor",
    "SupervisorGroup",
    "__version__",
    "build_app",
    "build_redis",
    "build_web_client",
    "claim_event",
    "enqueue",
    "from_stream_fields",
    "process_event",
    "register_handlers",
    "to_stream_fields",
]
