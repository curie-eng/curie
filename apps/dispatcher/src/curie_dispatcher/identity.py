"""Log which principal a Slack user is (#2910, ADR 0155 step 5).

For every inbound payload the dispatcher asks the platform API
(``POST /identity/resolve``, located by Slack team id) which principal the
acting Slack user is, and only LOGS the answer. Nothing else changes: no field
on the queued turn, no drop, and no delay.

The #1053/#1077 rule holds: nothing may sit before the ack. In a Bolt global
middleware ``next()`` only sets a flag, so code after it still runs before the
listener; the lookup is therefore submitted to a small background executor
before ``next()`` and the listener never waits on it. The dispatcher does not
import ``curie_api`` (ADR 0155 section 5); it uses HTTP, like
``ApprovalResolveClient``.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Protocol

import httpx

from .config import DispatcherConfig

# Its own logger, not the injected drop logger, so the #2006 drop-reason
# oracle never sees these records.
logger = logging.getLogger("curie_dispatcher.identity")

# Off the ack path, so this bounds a stuck worker rather than an ack.
_RESOLVE_TIMEOUT = httpx.Timeout(2.0)
# A lookup is a courtesy log line: under a burst, skip rather than queue.
_MAX_WORKERS = 2
_MAX_IN_FLIGHT = 16

# One pool for the process, not one per app: every built app would otherwise
# keep its own idle workers alive until interpreter exit.
_pool_lock = threading.Lock()
_pool: ThreadPoolExecutor | None = None
# Clients ``build_identity_client`` made, so shutdown can close their sockets.
_built_clients: list[httpx.Client] = []


class IdentityResolver(Protocol):
    def resolve_slack_user(self, team_id: str, user_id: str) -> dict[str, Any] | None: ...


class IdentityResolveClient:
    """Thin client for the API's principal resolution route."""

    def __init__(self, api_base_url: str, api_key: str, client: httpx.Client | None = None) -> None:
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._client = client or httpx.Client(timeout=_RESOLVE_TIMEOUT)

    def resolve_slack_user(self, team_id: str, user_id: str) -> dict[str, Any] | None:
        """The API's answer, or None when there is none to give. Never raises.

        The tenant is omitted, so the route uses the default one.
        """
        body = {"provider": "slack", "external_account_id": team_id, "provider_subject": user_id}
        try:
            response = self._client.post(
                f"{self._base}/identity/resolve", json=body, headers=self._headers
            )
        except Exception:
            # Not only HTTPError: a closed client or a custom transport can
            # raise anything, and this seam promises None.
            return None
        if response.status_code != 200:
            return None
        try:
            parsed = response.json()
        except ValueError:
            return None
        return parsed if isinstance(parsed, dict) else None


def build_identity_client(config: DispatcherConfig) -> IdentityResolveClient:
    """The production client, from the dispatcher's API settings."""
    http = httpx.Client(timeout=_RESOLVE_TIMEOUT)
    with _pool_lock:
        _built_clients.append(http)
    return IdentityResolveClient(config.api_base_url, config.api_key, client=http)


def _shared_pool() -> ThreadPoolExecutor:
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ThreadPoolExecutor(
                max_workers=_MAX_WORKERS, thread_name_prefix="identity-lookup"
            )
        return _pool


def shutdown_identity_lookups() -> None:
    """Drop queued lookups and close built clients when the dispatcher stops.

    Running lookups are not waited on, so a slow API cannot hold up exit. The
    next submit starts a fresh pool, so a restarted supervisor still logs.
    """
    global _pool
    with _pool_lock:
        pool, _pool = _pool, None
        clients = list(_built_clients)
        _built_clients.clear()
    if pool is not None:
        pool.shutdown(wait=False, cancel_futures=True)
    for http in clients:
        http.close()


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def slack_subject(body: dict[str, Any]) -> tuple[str, str] | None:
    """The (team id, user id) that acted, or None when the payload names no user.

    An Events API envelope carries ``team_id`` at the top and the user on
    ``event.user``; an interaction payload carries ``team.id`` and ``user.id``.
    No other field is consulted, so a bot message is None rather than a guess.
    """
    event = body.get("event")
    if isinstance(event, dict):
        team = _text(body.get("team_id"))
        user = _text(event.get("user"))
    else:
        team_block = body.get("team")
        user_block = body.get("user")
        team = _text(team_block.get("id")) if isinstance(team_block, dict) else None
        user = _text(user_block.get("id")) if isinstance(user_block, dict) else None
    if team is None or user is None:
        return None
    return team, user


def observe_principal(body: dict[str, Any], client: IdentityResolver, log: logging.Logger) -> None:
    """Look up and log the acting Slack user's principal. Never raises.

    The record names only the status, reason, principal id and the Slack team
    and user ids, never message text or a credential.
    """
    subject = slack_subject(body)
    if subject is None:
        return
    team, user = subject
    try:
        answer = client.resolve_slack_user(team, user)
    except Exception as exc:
        log.warning(
            "slack principal lookup failed team=%s user=%s error=%s",
            team,
            user,
            type(exc).__name__,
        )
        return
    if answer is None:
        log.warning("slack principal lookup failed team=%s user=%s", team, user)
        return
    log.info(
        "slack principal resolution status=%s reason=%s principal_id=%s team=%s user=%s",
        answer.get("status"),
        answer.get("reason"),
        answer.get("principal_id") or "-",
        team,
        user,
    )


def principal_observer_middleware(
    client: IdentityResolver,
    *,
    executor: ThreadPoolExecutor | None = None,
    log: logging.Logger = logger,
) -> Callable[..., Any]:
    """A Bolt global middleware that submits the lookup, then calls ``next_``.

    Extracting the subject is pure dict reads; the lookup itself runs on the
    executor. When ``_MAX_IN_FLIGHT`` lookups are pending the observation is
    skipped, so a slow API can neither queue work without bound nor slow a
    listener.
    """
    in_flight = threading.BoundedSemaphore(_MAX_IN_FLIGHT)

    def _observe(body: dict[str, Any]) -> None:
        observe_principal(body, client, log)

    def _release(_future: Future[None]) -> None:
        # A done-callback fires exactly once, including for a lookup cancelled
        # by shutdown, so the slot is never leaked or released twice.
        in_flight.release()

    def _middleware(body: dict[str, Any], next_: Callable[[], None]) -> None:
        try:
            if slack_subject(body) is not None:
                if in_flight.acquire(blocking=False):
                    # Looked up per submit, so a shutdown's fresh pool is used.
                    pool = executor or _shared_pool()
                    try:
                        future = pool.submit(_observe, body)
                    except Exception:
                        in_flight.release()
                        raise
                    future.add_done_callback(_release)
                else:
                    log.debug("slack principal lookup skipped: %d in flight", _MAX_IN_FLIGHT)
        except Exception as exc:
            log.warning("slack principal lookup not submitted error=%s", type(exc).__name__)
        next_()

    return _middleware
