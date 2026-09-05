"""Process entrypoint: read config, refuse to run half-configured, then serve.

The boot gates are the point of this module. The adapter's inbox is a public
mailbox by construction, so an install that comes up with ingress enabled and no
allow-list is an open trigger for agent turns available to anyone who learns the
address. Crashing at boot naming the variable is the same shape the dispatcher
already uses for its own unconfigurable-to-run state, and in Kubernetes
`CrashLoopBackOff` is the operator signal.

Run it with ``python -m curie_mail_adapter``.
"""

from __future__ import annotations

import logging
import os
import signal
import threading

from curie_telemetry import bootstrap_service_telemetry

from . import __version__
from .adapter import MailAdapter
from .config import MailAdapterConfig
from .egress import make_server

logger = logging.getLogger(__name__)


def boot_problems(config: MailAdapterConfig) -> list[str]:
    """Every reason this configuration cannot be served, each naming its env var."""
    problems = []
    for env_name, credential_value in (
        ("AGENTMAIL_INBOX", config.agentmail_inbox),
        ("AGENTMAIL_API_KEY", config.agentmail_api_key),
        ("CURIE_CHANNEL_TOKEN", config.channel_token),
        ("CURIE_EGRESS_SECRET", config.egress_secret),
    ):
        if not credential_value.strip():
            problems.append(f"{env_name} is required and is unset or empty")
    if config.poll_interval_seconds <= 0:
        problems.append(
            "CURIE_MAIL_POLL_INTERVAL_SECONDS must be greater than zero, "
            f"not {config.poll_interval_seconds}"
        )
    for env_name, bound_value in (
        ("CURIE_MAIL_MAX_PENDING_DELIVERIES", config.max_pending_deliveries),
        ("CURIE_MAIL_MAX_BODY_BYTES", config.max_body_bytes),
        ("CURIE_MAIL_MAX_REPLY_BYTES", config.max_reply_bytes),
        ("CURIE_MAIL_MAX_STATE_BYTES", config.max_state_bytes),
    ):
        if bound_value <= 0:
            problems.append(f"{env_name} must be greater than zero, not {bound_value}")
    if config.ingress_enabled and not config.allowed_senders:
        problems.append(
            "CURIE_MAIL_ALLOWED_SENDERS is required while ADAPTER_INGRESS_ENABLED is true: "
            "an empty list means deny everything, and it is refused rather than served "
            "as deny-all. Write '*' to accept mail from anyone."
        )
    return problems


def main() -> None:
    # The *package* logger is handed over, not this module's. `run`, `adapter`
    # and `egress` each hold their own `getLogger(__name__)` child of it, and
    # `configure_service_logging` sets `propagate=False` on exactly the logger it
    # is given — so one bootstrap installs the redacting JSON handler for all
    # three and nothing walks past it to a root handler. Bootstrapping the three
    # module loggers instead would leave a fourth module silently unredacted.
    telemetry = bootstrap_service_telemetry(
        "curie-mail-adapter",
        service_version=__version__,
        logger=logging.getLogger("curie_mail_adapter"),
        environ=os.environ,
    )
    try:
        config = MailAdapterConfig()

        problems = boot_problems(config)
        if problems:
            for problem in problems:
                logger.error("%s", problem)
            # Inside the outer try on purpose: the outer `finally` force-flushes
            # these records before exit, and a crash-looping pod's boot error is
            # the one log an operator most needs to have been exported.
            raise SystemExit(1)

        adapter = MailAdapter(config)
        server = make_server(adapter, config.port)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        logger.info(
            "mail adapter starting: kind=email port=%s ingress_enabled=%s",
            server.server_address[1],
            config.ingress_enabled,
        )

        def _handle_signal(signum: int, _frame: object) -> None:
            logger.info("received signal %s, shutting down", signum)
            adapter.shutdown.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        try:
            if config.ingress_enabled:
                adapter.poll_loop()
            else:
                # The flag gates the poller, never the server: a staged cutover
                # serves replies before it starts ingesting.
                logger.info("ingress disabled; serving egress only")
                adapter.ready.set()
            adapter.shutdown.wait()
        finally:
            server.shutdown()
            server.server_close()
            adapter.close()
        logger.info("mail adapter stopped")
    finally:
        # After the stop record is emitted, never before: shutdown force-flushes,
        # so flushing first would drop the last record of every graceful stop.
        telemetry.shutdown()


if __name__ == "__main__":
    main()
