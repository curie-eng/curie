"""Process entrypoint: wire config, clients, apps, and supervisors, then run.

This is the top-level composition. It reads the environment, runs the boot
gates, builds one Bolt app and Socket Mode supervisor per Slack identity that
passed preflight (ADR-0168 decision 2), installs SIGINT/SIGTERM handlers for
graceful shutdown, and runs the supervisors together. Run it with
``python -m curie_dispatcher``.
"""

import logging
import os
import signal
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import redis
from curie_telemetry import bootstrap_service_telemetry
from slack_bolt import App
from slack_sdk.web import WebClient

from . import __version__
from .app import SocketModeConnection, build_app, build_redis, build_web_client
from .config import DispatcherConfig
from .heartbeat import start_heartbeat
from .identities import (
    SlackBotIds,
    SlackIdentityCredentials,
    default_identity_credentials,
    resolve_identity_credentials,
)
from .preflight import (
    ApiUnreachableError,
    PreflightedIdentity,
    SlackChannelPreflightError,
    check_api_reachable,
    check_slack_channel_capabilities,
)
from .supervisor import BackoffPolicy, Supervisor, SupervisorGroup


@dataclass(frozen=True)
class IdentityConnection:
    """One Slack identity's live pieces.

    Its own Web API client, Bolt app and Socket Mode supervisor, so every
    placeholder, card stamp and ephemeral is made with the token of the app the
    delivery arrived on, and one identity's reconnects never touch another's.
    ``bot_ids`` is what preflight's ``auth.test`` reported, held for decision 6.
    """

    name: str
    web_client: WebClient
    app: App
    connect: Callable[[], SocketModeConnection]
    supervisor: Supervisor
    bot_ids: SlackBotIds | None


def _socket_mode_connector(
    app: App,
    credentials: SlackIdentityCredentials,
    *,
    logger: logging.Logger,
    slack_identity: str | None,
) -> Callable[[], SocketModeConnection]:
    def connect() -> SocketModeConnection:
        return SocketModeConnection(
            app, credentials.app_token, logger=logger, slack_identity=slack_identity
        )

    return connect


def build_identity_connections(
    config: DispatcherConfig,
    identities: Sequence[PreflightedIdentity],
    *,
    redis_client: redis.Redis,
    logger: logging.Logger,
) -> tuple[IdentityConnection, ...]:
    """One connection per identity, all feeding the one stream ``redis_client`` writes.

    A lone identity is built exactly as the single app always was: no label on
    its supervisor, no identity on its connection's log lines.
    """
    several = len(identities) > 1
    backoff = BackoffPolicy(
        initial_seconds=config.backoff_initial_seconds,
        max_seconds=config.backoff_max_seconds,
        multiplier=config.backoff_multiplier,
    )
    connections: list[IdentityConnection] = []
    for preflighted in identities:
        credentials = preflighted.credentials
        web_client = build_web_client(config, credentials)
        app = build_app(
            config,
            identity=credentials,
            web_client=web_client,
            redis_client=redis_client,
            logger=logger,
        )
        connect = _socket_mode_connector(
            app,
            credentials,
            logger=logger,
            slack_identity=credentials.name if several else None,
        )
        supervisor = Supervisor(
            connect,
            backoff=backoff,
            logger=logger,
            label=f"Slack identity {credentials.name}" if several else None,
        )
        connections.append(
            IdentityConnection(
                name=credentials.name,
                web_client=web_client,
                app=app,
                connect=connect,
                supervisor=supervisor,
                bot_ids=preflighted.bot_ids,
            )
        )
    return tuple(connections)


def build_supervisor(
    config: DispatcherConfig,
    *,
    logger: logging.Logger,
    identities: Sequence[PreflightedIdentity] | None = None,
) -> SupervisorGroup:
    """Assemble one supervisor per admitted identity, run together.

    ``identities`` defaults to ``default`` from the settings, the stock install.
    """
    admitted = (
        tuple(identities)
        if identities is not None
        else (PreflightedIdentity(default_identity_credentials(config), None),)
    )
    connections = build_identity_connections(
        config, admitted, redis_client=build_redis(config), logger=logger
    )
    return SupervisorGroup({c.name: c.supervisor for c in connections})


def main() -> None:
    logger = logging.getLogger("curie_dispatcher")
    telemetry = bootstrap_service_telemetry(
        "curie-dispatcher",
        service_version=__version__,
        logger=logger,
        environ=os.environ,
    )
    try:
        # BaseSettings supplies the required chat-attester secret from the
        # environment; mypy sees only the constructor signature, not that
        # runtime settings source.
        config = DispatcherConfig()  # type: ignore[call-arg]

        # Gate on the platform API wiring before touching Slack: a dispatcher that
        # cannot reach the API dead-ends every approval click (#442).
        try:
            check_api_reachable(config, logger=logger)
        except ApiUnreachableError as exc:
            logger.error("%s", exc)
            raise SystemExit(1) from exc

        identities = resolve_identity_credentials(config, logger=logger)
        try:
            admitted = check_slack_channel_capabilities(
                config,
                logger=logger,
                identities=identities,
            )
        except SlackChannelPreflightError as exc:
            logger.error("%s", exc)
            raise SystemExit(1) from exc

        supervisor = build_supervisor(config, logger=logger, identities=admitted)
        hb_stop = start_heartbeat(config.heartbeat_file, config.heartbeat_interval_s)

        def _handle_signal(signum: int, _frame: object) -> None:
            logger.info("received signal %s, shutting down", signum)
            hb_stop.set()
            supervisor.request_stop()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        logger.info("dispatcher starting")
        try:
            supervisor.run()
        finally:
            hb_stop.set()
        logger.info("dispatcher stopped")
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
