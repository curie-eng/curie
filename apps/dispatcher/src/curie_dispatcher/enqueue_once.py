"""Bounded, Slack-free dispatcher producer for local CLI turns.

``curie local message`` uses this module through ``docker compose run`` so its
synthetic turn crosses the same dispatcher-owned producer span and transport
carrier as a Slack turn.  The serialized ``QueuedTurn`` arrives on stdin and
the assigned Stream id is the only stdout output.  Socket Mode and the Slack
clients are deliberately not constructed.
"""

from __future__ import annotations

import logging
import os
import sys

from aci_protocol import QueuedTurn, parse_queued_turn
from curie_telemetry import bootstrap_service_telemetry

from . import __version__
from .app import build_redis
from .config import DispatcherConfig
from .queue import StreamPublisher, enqueue

_LOG = logging.getLogger("curie_dispatcher.enqueue_once")


def enqueue_payload(
    payload: str,
    *,
    config: DispatcherConfig,
    redis_client: StreamPublisher,
) -> str:
    """Validate one frozen turn payload and enqueue it through the producer."""
    turn: QueuedTurn = parse_queued_turn(payload)
    return enqueue(redis_client, config, turn)


def main() -> int:
    """Read one turn from stdin, enqueue it, and return a process exit code.

    Failure diagnostics intentionally name only the failed stage and exception
    type.  Pydantic validation errors include rejected input values, and Redis
    connection strings may contain credentials, so echoing either exception's
    text would turn the CLI's stderr preservation into a secret leak.
    """
    telemetry = bootstrap_service_telemetry(
        "curie-dispatcher",
        service_version=__version__,
        logger=logging.getLogger("curie_dispatcher"),
        environ=os.environ,
    )
    try:
        payload = sys.stdin.read()
        try:
            config = DispatcherConfig()
            redis_client = build_redis(config)
            stream_id = enqueue_payload(
                payload,
                config=config,
                redis_client=redis_client,
            )
        except Exception as exc:
            _LOG.error(
                "one-shot dispatcher enqueue failed (%s)",
                type(exc).__name__,
            )
            return 1
        sys.stdout.write(f"{stream_id}\n")
        return 0
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
