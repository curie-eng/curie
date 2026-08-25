"""Process entrypoint: wire config, clients, app, and supervisor, then run.

This is the top-level composition. It reads the environment, builds the Bolt app
and its backing clients, installs SIGINT/SIGTERM handlers for graceful shutdown,
and hands a Socket Mode connection factory to the supervisor. Run it with
``python -m curie_dispatcher``.
"""

import logging
import os
import signal

from curie_telemetry import bootstrap_service_telemetry

from . import __version__
from .app import SocketModeConnection, build_app, build_redis, build_web_client
from .config import DispatcherConfig
from .heartbeat import start_heartbeat
from .preflight import ApiUnreachableError, check_api_reachable
from .supervisor import BackoffPolicy, Supervisor


def build_supervisor(config: DispatcherConfig, *, logger: logging.Logger) -> Supervisor:
    """Assemble the supervisor and its Socket Mode connection factory from config."""
    redis_client = build_redis(config)
    web_client = build_web_client(config)
    app = build_app(config, web_client=web_client, redis_client=redis_client, logger=logger)

    def connect() -> SocketModeConnection:
        return SocketModeConnection(app, config.slack_app_token, logger=logger)

    backoff = BackoffPolicy(
        initial_seconds=config.backoff_initial_seconds,
        max_seconds=config.backoff_max_seconds,
        multiplier=config.backoff_multiplier,
    )
    return Supervisor(connect, backoff=backoff, logger=logger)


def main() -> None:
    logger = logging.getLogger("curie_dispatcher")
    telemetry = bootstrap_service_telemetry(
        "curie-dispatcher",
        service_version=__version__,
        logger=logger,
        environ=os.environ,
    )
    try:
        config = DispatcherConfig()

        # Gate on the platform API wiring before touching Slack: a dispatcher that
        # cannot reach the API dead-ends every approval click (#442).
        try:
            check_api_reachable(config, logger=logger)
        except ApiUnreachableError as exc:
            logger.error("%s", exc)
            raise SystemExit(1) from exc

        supervisor = build_supervisor(config, logger=logger)
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
