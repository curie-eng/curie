"""Construction of the runtime pieces: Valkey client, Web client, Bolt app, and
the Socket Mode connection the supervisor drives.

These are thin factories so the interesting logic (handlers, supervisor) stays
testable in isolation. ``build_app`` accepts an optional ``authorize`` callback:
in production the Bolt app authorizes with the real bot token; tests pass a stub
authorize to keep the dispatch path offline.
"""

import logging
import threading
from collections.abc import Callable
from typing import Any

import redis
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.web import WebClient

from .config import DispatcherConfig
from .handlers import Clock, register_handlers
from .supervisor import Connection

# In Socket Mode the signing secret is never used to verify requests (they arrive
# over the authenticated websocket, not HTTP), but Bolt's App still wants one at
# construction. A placeholder is therefore harmless.
_SOCKET_MODE_SIGNING_PLACEHOLDER = "unused-in-socket-mode"

# How long any one phase of a Slack Web API call may take (#1077). slack_sdk
# defaults this to 30 seconds (measured on slack_sdk 3.43.0,
# ``slack_sdk.web.base_client.BaseClient.__init__``), an order of magnitude past
# Slack's three second interaction deadline, so a single slow call could hold one
# of Bolt's five shared listener-executor workers for most of a minute.
#
# What two seconds buys is that bounded hold, and nothing stronger. It is NOT a
# two second ceiling on a call: slack_sdk's default ``retry_handlers`` include a
# ``ConnectionErrorRetryHandler(max_retry_count=1)``, and urllib raises a
# connect/send-phase timeout as a ``URLError``, which that handler retries once
# after a jittered backoff. The worst case for such a call is therefore about
# 2 + 0.5 + 2 = 4.5s, past the three second deadline. (A read-phase timeout
# surfaces as ``TimeoutError``, which the handler does not match, so that phase
# is not retried.) Do not read this constant as making a Web API call safe to put
# back on a pre-ack path -- nothing on the ack path may call Slack at all.
_SLACK_API_TIMEOUT_SECONDS = 2


def build_redis(config: DispatcherConfig) -> redis.Redis:
    """A decode_responses Valkey client (str in, str out) for stream + dedupe ops."""
    return redis.Redis(
        host=config.valkey_host,
        port=config.valkey_port,
        password=config.valkey_password or None,
        db=config.valkey_db,
        decode_responses=True,
        ssl=config.valkey_tls,
    )


def build_web_client(config: DispatcherConfig) -> WebClient:
    """The dispatcher's own Web API client, authenticated with the bot token."""
    return WebClient(token=config.slack_bot_token, timeout=_SLACK_API_TIMEOUT_SECONDS)


def build_app(
    config: DispatcherConfig,
    *,
    web_client: WebClient,
    redis_client: redis.Redis,
    clock: Clock | None = None,
    authorize: Callable[..., Any] | None = None,
    logger: logging.Logger | None = None,
    resolver: Any | None = None,
) -> App:
    """Build a Bolt App with the dispatcher's handlers registered."""
    signing = config.slack_signing_secret or _SOCKET_MODE_SIGNING_PLACEHOLDER
    app_kwargs: dict[str, Any] = {}
    app_kwargs["signing_secret"] = signing
    if authorize is not None:
        app_kwargs["authorize"] = authorize
    else:
        app_kwargs["token"] = config.slack_bot_token
        # Defer token validation to connect time: a bare token otherwise makes
        # Bolt call auth.test eagerly at construction, which would require network
        # to build the app and fail startup on a transient Slack blip. Socket Mode
        # connects via the app token and the supervisor owns reconnect, so the
        # connection is the source of truth for token validity.
        app_kwargs["token_verification_enabled"] = False

    app = App(**app_kwargs)
    register_kwargs: dict[str, Any] = {
        "web_client": web_client,
        "redis_client": redis_client,
        "config": config,
        "logger": logger,
    }
    if clock is not None:
        register_kwargs["clock"] = clock
    if resolver is not None:
        # The approvals API client (#246), injectable so tests keep the
        # click-to-resolve path offline.
        register_kwargs["resolver"] = resolver
    register_handlers(app, **register_kwargs)
    return app


class SocketModeConnection(Connection):
    """Adapts Bolt's SocketModeHandler to the supervisor's Connection protocol.

    ``run`` connects and then blocks on an internal event; the builtin client
    reconnects transient websocket drops itself, so ``run`` returns only on
    graceful ``close`` or if ``connect`` raises (which the supervisor treats as a
    reconnect-with-backoff trigger).
    """

    def __init__(
        self,
        app: App,
        app_token: str,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._handler = SocketModeHandler(app, app_token=app_token)
        self._logger = logger or logging.getLogger(__name__)
        self._closed = threading.Event()

    def run(self) -> None:
        self._closed.clear()
        self._handler.connect()  # type: ignore[no-untyped-call]
        self._logger.info("socket mode connected")
        self._closed.wait()

    def close(self) -> None:
        self._closed.set()
        try:
            self._handler.close()  # type: ignore[no-untyped-call]
        except Exception:  # pragma: no cover - best-effort teardown
            self._logger.exception("error closing socket mode handler")
