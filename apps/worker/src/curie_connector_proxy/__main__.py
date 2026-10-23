"""``python -m curie_connector_proxy``: serve the caller proxy from the render's env.

``server.shared_mode`` picks the mode: shared application mode (``shared.py``,
ADR 0239) when the shared render's ``CURIE_SHARED_APPLICATION`` is present,
dedicated mode otherwise.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Awaitable

from aiohttp import web

from .resolver.config import ResolverConfig
from .resolver.http import ResolverStartRefused, start_resolver
from .server import ProxyConfig, make_app, shared_mode
from .shared import SharedProxyConfig, build_shared_app


def _run(app: web.Application | Awaitable[web.Application], port: int) -> None:
    web.run_app(
        app,
        host="0.0.0.0",  # noqa: S104 - the Service and the rendered policies select this port
        port=port,
        handler_cancellation=True,
        print=None,
    )


def _serve_shared() -> int:
    try:
        config = SharedProxyConfig.from_env(os.environ)
        resolver_config = ResolverConfig.from_env(os.environ)
    except ValueError as exc:
        # The closed failure, as in dedicated mode: nothing listens.
        print(f"curie_connector_proxy: {exc}", file=sys.stderr)
        return 2

    async def application() -> web.Application:
        # Awaited by ``run_app`` before any site is started, so a resolver that
        # refuses to start, or no published caller verify key
        # (``caller_keys_unavailable``), aborts startup before the proxy port
        # listens.
        runtime = await start_resolver(resolver_config)
        try:
            app = await build_shared_app(config, runtime)
        except BaseException:
            await runtime.close()
            raise

        async def close_resolver(_app: web.Application) -> None:
            await runtime.close()

        app.on_cleanup.append(close_resolver)
        return app

    try:
        _run(application(), config.listen_port)
    except ResolverStartRefused as refused:
        # The code names the failed check, never a value.
        print(f"curie_connector_proxy: the resolver refused to start: {refused}", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    if shared_mode(os.environ):
        return _serve_shared()
    try:
        config = ProxyConfig.from_env(os.environ)
    except ValueError as exc:
        # Refusing to start is the closed failure: nothing listens on the
        # proxy port, so no caller reaches the server either.
        print(f"curie_connector_proxy: {exc}", file=sys.stderr)
        return 2
    _run(make_app(config), config.listen_port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
