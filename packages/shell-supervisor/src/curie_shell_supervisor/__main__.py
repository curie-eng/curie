"""Run the supervisor as the shell's PID 1: ``python -m curie_shell_supervisor``."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import ssl
import sys
from pathlib import Path

from aiohttp import web

from .child import become_subreaper
from .config import ConfigError, SupervisorConfig, check_private
from .server import Supervisor

logger = logging.getLogger("curie_shell_supervisor")


def server_ssl_context(config: SupervisorConfig) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(config.tls_cert_file, config.tls_key_file)
    return context


async def serve(config: SupervisorConfig) -> None:
    loop = asyncio.get_running_loop()
    supervisor = Supervisor(config)

    def on_loop_error(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        # A task or future that failed unobserved is a supervisor fault: the
        # shell dies whole rather than keep a child nobody guards. A client's
        # failed TLS handshake or dropped connection reaches this handler too,
        # and must not let any peer on the network stop the shell.
        if "future" in context or "task" in context:
            supervisor.die(str(context.get("message") or context.get("exception")))
            return
        logger.warning("connection fault: %s", context.get("message"))

    loop.set_exception_handler(on_loop_error)
    runner = web.AppRunner(supervisor.app(), handler_cancellation=True, access_log=None)
    await runner.setup()
    site = web.TCPSite(
        runner, config.listen_host, config.port, ssl_context=server_ssl_context(config)
    )
    await site.start()
    logger.info(
        "shell %s incarnation %s serving on port %d",
        config.slot,
        config.incarnation,
        config.port,
    )
    stopping = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stopping.set)
    await stopping.wait()
    # Orderly stop: cleanup stops the child group before the process exits.
    await runner.cleanup()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("CURIE_SUPERVISOR_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        token_file = os.environ.get("CURIE_SUPERVISOR_CONTROL_TOKEN_FILE")
        config = SupervisorConfig.from_env(os.environ)
        for private in (config.tls_key_file, *([Path(token_file)] if token_file else [])):
            check_private(private, child_uid=config.child_uid, child_gid=config.child_gid)
    except (ConfigError, OSError) as exc:
        logger.critical("supervisor configuration refused: %s", exc)
        sys.exit(78)
    become_subreaper()
    try:
        asyncio.run(serve(config))
    except Exception:
        logger.exception("supervisor stopped on an unhandled fault")
        sys.exit(70)


if __name__ == "__main__":
    main()
