"""Process entrypoint: read config, load the provider, refuse to run half-configured.

The boot gates require a deliberate sender filter, including explicit consent
to a wildcard. That filter never authenticates a sender: only a provider that
verifies senders can open the channel (ADR 0228), and the built-in AgentMail
provider does not, so an AgentMail install refuses all inbound mail. In
Kubernetes `CrashLoopBackOff` is the configuration error signal.

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
from .agentmail import AgentMailProvider
from .config import MailAdapterConfig
from .egress import make_server
from .provider import MailProvider, ProviderLoadError, provider_problems, resolve_factory

logger = logging.getLogger(__name__)


def load_provider(config: MailAdapterConfig) -> tuple[MailProvider | None, list[str]]:
    """Build the configured mail provider, or say why it cannot boot.

    Returns:
        The provider, or None, and every problem found with it, each naming its
        variable.
    """
    try:
        factory = resolve_factory(config.mail_provider, lambda: AgentMailProvider(config))
        provider = factory()
    except ProviderLoadError as error:
        return None, [str(error)]
    except Exception as error:  # noqa: BLE001 - a broken factory is a boot error
        return None, [
            f"CURIE_MAIL_PROVIDER={config.mail_provider!r}: building the provider failed: "
            f"{type(error).__name__}"
        ]
    return provider, provider_problems(provider)


def boot_problems(config: MailAdapterConfig) -> list[str]:
    """Every reason the neutral configuration cannot be served, each naming its env var."""
    problems = []
    for env_name, credential_value in (
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
    if config.discovery_unready_after_seconds <= 0:
        problems.append(
            "CURIE_MAIL_DISCOVERY_UNREADY_AFTER_SECONDS must be greater than zero, "
            f"not {config.discovery_unready_after_seconds}"
        )
    if config.ingress_enabled and not config.allowed_senders:
        problems.append(
            "CURIE_MAIL_ALLOWED_SENDERS is required while ADAPTER_INGRESS_ENABLED is true: "
            "configure explicit sender addresses or domains; an empty list is refused"
        )
    if "*" in config.allowed_senders and not config.allow_all_senders:
        problems.append(
            "CURIE_MAIL_ALLOWED_SENDERS contains a wildcard: "
            "CURIE_MAIL_ALLOW_ALL_SENDERS must be explicitly true to permit it; "
            "the sender filter never replaces verifiable authentication"
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

        provider, problems = load_provider(config)
        problems = problems + boot_problems(config)
        if problems or provider is None:
            for problem in problems:
                logger.error("%s", problem)
            # Inside the outer try on purpose: the outer `finally` force-flushes
            # these records before exit, and a crash-looping pod's boot error is
            # the one log an operator most needs to have been exported.
            raise SystemExit(1)

        try:
            adapter = MailAdapter(config, provider=provider)
        except RuntimeError as error:
            # The state file belongs to another provider: ids from two mail
            # services must never mix, so this is a configuration error.
            logger.error("%s", error)
            raise SystemExit(1) from error
        server = make_server(adapter, config.port)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        logger.info(
            "mail adapter starting: kind=email port=%s provider=%s ingress_enabled=%s",
            server.server_address[1],
            provider.name,
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
