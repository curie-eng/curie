"""``python -m curie_connector_proxy``: serve the caller proxy from the render's env."""

from __future__ import annotations

import logging
import os
import sys

from aiohttp import web

from .server import ProxyConfig, make_app


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        config = ProxyConfig.from_env(os.environ)
    except ValueError as exc:
        # Refusing to start is the closed failure: nothing listens on the
        # proxy port, so no caller reaches the server either.
        print(f"curie_connector_proxy: {exc}", file=sys.stderr)
        return 2
    web.run_app(
        make_app(config),
        host="0.0.0.0",  # noqa: S104 - the Service and the rendered policies select this port
        port=config.listen_port,
        handler_cancellation=True,
        print=None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
