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

from .admission import AdmissionGate
from .config import DispatcherConfig, release_identity
from .handlers import Clock, register_handlers
from .identities import SlackIdentityCredentials, default_identity_credentials
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


def build_web_client(
    config: DispatcherConfig, identity: SlackIdentityCredentials | None = None
) -> WebClient:
    """One identity's own Web API client, authenticated with its bot token.

    ``default``, from the ``SLACK_*`` settings, when no identity is given.
    """
    credentials = identity if identity is not None else default_identity_credentials(config)
    return WebClient(token=credentials.bot_token, timeout=_SLACK_API_TIMEOUT_SECONDS)


def build_app(
    config: DispatcherConfig,
    *,
    web_client: WebClient,
    redis_client: redis.Redis,
    identity: SlackIdentityCredentials | None = None,
    clock: Clock | None = None,
    authorize: Callable[..., Any] | None = None,
    logger: logging.Logger | None = None,
    resolver: Any | None = None,
    admission: AdmissionGate | None = None,
) -> App:
    """Build one identity's Bolt App with the dispatcher's handlers registered.

    ``identity`` is ``default`` when omitted, built from the ``SLACK_*``
    settings exactly as a stock install always built it. Its name is what every
    turn this app mints carries (ADR-0168 decision 2).

    ``admission`` is the caller-list gate (ADR 0175). ``run`` passes one gate
    shared by every identity; None builds one for this app from config.
    """
    credentials = identity if identity is not None else default_identity_credentials(config)
    signing = credentials.signing_secret or _SOCKET_MODE_SIGNING_PLACEHOLDER
    app_kwargs: dict[str, Any] = {}
    app_kwargs["signing_secret"] = signing
    if authorize is not None:
        app_kwargs["authorize"] = authorize
    else:
        app_kwargs["token"] = credentials.bot_token
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
        "slack_identity": credentials.name,
    }
    if clock is not None:
        register_kwargs["clock"] = clock
    if resolver is not None:
        # The approvals API client (#246), injectable so tests keep the
        # click-to-resolve path offline.
        register_kwargs["resolver"] = resolver
    if admission is not None:
        register_kwargs["admission"] = admission
    register_handlers(app, **register_kwargs)
    return app


# How long closing a connection waits for the builtin client's session runner
# to finish its current pass. A pass is bounded by the socket's receive timeout
# (slack_sdk's builtin ``Connection`` defaults it to 3 seconds) or a 0.2 second
# idle sleep; past this, the daemon thread is left rather than hanging shutdown.
_SESSION_RUNNER_JOIN_S = 5.0


def _stop_session_runner(client: Any) -> None:
    """Stop the session runner thread the builtin Socket Mode client leaves up.

    Observed on slack_sdk 3.44.1, probing a ``SocketModeHandler`` built over a
    token-verification-free Bolt ``App``:

    - ``close()`` stops the app monitor, the message processor and the worker
      pool, but ``client.current_session_runner.is_alive()`` is still True
      afterwards, so each connection built leaves one thread behind.
    - With a session installed (a socketpair standing in for the websocket),
      ``close()`` then setting the runner's event and joining it for 3 s
      leaves it alive: the pass in ``run_until_completion`` loops while its
      session state is not terminated, sleeping while the socket is gone.
      Marking ``current_session_state.terminated`` first let the join finish
      in under a second.

    Every attribute is slack_sdk internals, so each is looked up guardedly.
    """
    state = getattr(client, "current_session_state", None)
    if state is not None:
        state.terminated = True
    runner = getattr(client, "current_session_runner", None)
    event = getattr(runner, "event", None)
    if isinstance(event, threading.Event):
        event.set()
    thread = getattr(runner, "thread", None)
    if isinstance(thread, threading.Thread) and thread.is_alive():
        thread.join(_SESSION_RUNNER_JOIN_S)


class SocketModeConnection(Connection):
    """Adapts Bolt's SocketModeHandler to the supervisor's Connection protocol.

    ``run`` connects and then blocks on an internal event; the builtin client
    reconnects transient websocket drops itself, so ``run`` returns only on
    graceful ``close`` or if ``connect`` raises (which the supervisor treats as a
    reconnect-with-backoff trigger).

    ``slack_identity`` names the identity this connection serves when several
    run in one process (ADR-0168 decision 2). Without it, its log lines are
    unchanged.
    """

    def __init__(
        self,
        app: App,
        app_token: str,
        *,
        logger: logging.Logger | None = None,
        slack_identity: str | None = None,
    ) -> None:
        self._handler = SocketModeHandler(app, app_token=app_token)
        self._logger = logger or logging.getLogger(__name__)
        self._slack_identity = slack_identity
        self._closed = threading.Event()
        self._handler.client.message_listeners.append(self._on_socket_message)

    def _on_socket_message(
        self, client: Any, message: dict[str, Any], raw_message: Any
    ) -> None:
        """Warn when Slack reports more than one Socket Mode client on this app.

        Hello ``num_connections`` is the only runtime competition signal. Warn
        and keep the connection: this ticket detects overlap, it does not refuse
        connect or post to Slack.
        """
        del client, raw_message
        if message.get("type") != "hello":
            return
        raw = message.get("num_connections")
        if not isinstance(raw, (int, str)):
            return
        try:
            num_connections = int(raw)
        except ValueError:
            return
        if num_connections <= 1:
            return
        if self._slack_identity is None:
            self._logger.warning(
                "%s: exactly one Curie release may connect to a given Slack app; "
                "disconnect extra clients",
                release_identity(),
            )
        else:
            self._logger.warning(
                "%s: exactly one Curie release may connect to a given Slack app; "
                "disconnect extra clients of Slack identity %s",
                release_identity(),
                self._slack_identity,
            )

    def run(self) -> None:
        # The supervisor never reuses a connection, so a close that landed
        # before run is final: honour it rather than clearing it.
        if self._closed.is_set():
            return
        try:
            self._handler.connect()  # type: ignore[no-untyped-call]
        except BaseException:
            # The handler started its client's threads at construction; a
            # connect that fails would otherwise leave them running, one set
            # per reconnect attempt.
            self.close()
            raise
        if self._closed.is_set():
            # A close landed while connect was in flight and tore down a
            # handler connect then brought back up; close what it opened.
            self.close()
            return
        if self._slack_identity is None:
            self._logger.info("socket mode connected identity=%s", release_identity())
        else:
            self._logger.info(
                "socket mode connected identity=%s slack_identity=%s",
                release_identity(),
                self._slack_identity,
            )
        self._closed.wait()

    def close(self) -> None:
        self._closed.set()
        try:
            self._handler.close()  # type: ignore[no-untyped-call]
        except Exception:  # pragma: no cover - best-effort teardown
            self._logger.exception("error closing socket mode handler")
        try:
            _stop_session_runner(self._handler.client)
        except Exception:  # pragma: no cover - best-effort teardown
            self._logger.exception("error stopping the socket mode session runner")
