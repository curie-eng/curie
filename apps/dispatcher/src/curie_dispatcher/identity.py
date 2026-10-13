"""Log which principal a Slack sender is (#2910, ADR 0198, ADR 0201).

For every inbound delivery the dispatcher pulls the sender evidence ADR 0201
maps out of the payload, asks the platform API (``POST /identity/resolve``,
named by the Slack identity the delivery arrived on) which principal that is,
and only LOGS the answer. Nothing is enforced yet (#2914): no field on the
queued turn, no drop, and no delay.

The #1053/#1077 rule holds: nothing may sit before the ack. In a Bolt global
middleware ``next()`` only sets a flag, so code after it still runs before the
listener; the lookup is therefore submitted to a small shared background pool
BEFORE ``next()``, and neither the ack nor the listener ever waits on it. The
dispatcher does not import ``curie_api`` (ADR 0155 section 5); it uses HTTP,
like ``AdmissionClient``.

Deciding who the sender is belongs to the API (``curie_api.identity.slack``).
This module only copies fields; if you find a comparison of team ids here, it
is a second copy of the API's rule and will drift.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Protocol

import httpx
from curie_telemetry import inject_trace_context

from .config import DispatcherConfig

# Its own logger, not the injected drop logger, so the #2006 drop-reason
# oracle never sees these records.
logger = logging.getLogger("curie_dispatcher.identity")

PROVIDER = "slack"
# Put into ``enterprise_ids`` for an ``is_enterprise_install: true`` that names
# no enterprise id, so the API refuses as it does for any enterprise id.
ENTERPRISE_INSTALL_SIGNAL = "is_enterprise_install"

# Every phase named, as in admission: a phase left at its default silently
# joins the sum. The call runs on the lookup pool, off the ack path, so this
# bounds how long a stuck API holds one of the pool's workers.
_RESOLVE_TIMEOUT = httpx.Timeout(connect=0.5, read=1.5, write=0.5, pool=0.5)
# A lookup is a log line: under a burst, skip rather than queue without bound.
_MAX_WORKERS = 2
_MAX_IN_FLIGHT = 16

# One pool and one in-flight bound for the process, not one per app: every
# identity's app would otherwise keep its own idle workers alive until exit.
_pool_lock = threading.Lock()
_pool: ThreadPoolExecutor | None = None
_in_flight = threading.BoundedSemaphore(_MAX_IN_FLIGHT)
# HTTP clients ``build_identity_client`` made, so shutdown can close their sockets.
_built_clients: list[httpx.Client] = []

# An API that is down fails every lookup: at most one lookup-failure WARNING
# per window per process, the next one carrying how many were suppressed.
_FAILURE_WARNING_WINDOW_S = 60.0
_monotonic = time.monotonic
_failure_lock = threading.Lock()
_failure_last_warned: float | None = None
_failure_suppressed = 0


def _failure_warning_due() -> tuple[bool, int]:
    """Whether to log this failure, and how many were suppressed before it."""
    global _failure_last_warned, _failure_suppressed
    now = _monotonic()
    with _failure_lock:
        if (
            _failure_last_warned is not None
            and now - _failure_last_warned < _FAILURE_WARNING_WINDOW_S
        ):
            _failure_suppressed += 1
            return False, 0
        suppressed, _failure_suppressed = _failure_suppressed, 0
        _failure_last_warned = now
        return True, suppressed


class IdentityResolver(Protocol):
    def resolve(self, identity_name: str, evidence: dict[str, Any]) -> dict[str, Any] | None: ...


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _block(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def slack_evidence(body: dict[str, Any]) -> dict[str, Any] | None:
    """The ADR 0201 sender evidence in ``body``, shaped as the API's ``SlackEvidence``.

    Fields are copied, never interpreted: the API decides which one names the
    sender's team. ``is_ext_shared_channel`` is kept only as a strict bool,
    because the API's mention path needs it to be exactly False. Every
    enterprise signal (an enterprise id anywhere, a ``context_enterprise_id``,
    or ``is_enterprise_install: true``) goes into ``enterprise_ids``. None when the
    delivery names no user, so a bot message is skipped rather than guessed.
    """
    event = body.get("event")
    enterprise_ids: set[str] = set()

    def collect(value: Any) -> None:
        if (text := _text(value)) is not None:
            enterprise_ids.add(text)

    def enterprise_install(block: dict[str, Any]) -> None:
        # An enterprise install is a Grid signal even without an id.
        if block.get("is_enterprise_install") is True:
            enterprise_ids.add(ENTERPRISE_INSTALL_SIGNAL)

    collect(body.get("enterprise_id"))
    collect(body.get("context_enterprise_id"))
    enterprise_install(body)
    authorizations = body.get("authorizations")
    if isinstance(authorizations, list):
        for authorization in authorizations:
            collect(_block(authorization).get("enterprise_id"))
            enterprise_install(_block(authorization))
    collect(_block(body.get("enterprise")).get("id"))
    collect(_block(body.get("team")).get("enterprise_id"))

    if isinstance(event, dict):
        user_id = _text(event.get("user"))
        collect(event.get("enterprise_id"))
        shared = body.get("is_ext_shared_channel")
        evidence: dict[str, Any] = {
            "delivery": _text(event.get("type")) or "",
            "user_id": user_id,
            "event_user_team": _text(event.get("user_team")),
            "event_team": _text(event.get("team")),
            "is_ext_shared_channel": shared if isinstance(shared, bool) else None,
        }
    else:
        user = _block(body.get("user"))
        user_id = _text(user.get("id"))
        collect(user.get("enterprise_id"))
        evidence = {
            "delivery": _text(body.get("type")) or "",
            "user_id": user_id,
            "interaction_user_team_id": _text(user.get("team_id")),
        }
    if user_id is None:
        return None
    evidence["enterprise_ids"] = sorted(enterprise_ids)
    return evidence


class IdentityResolveClient:
    """Thin client for the API's ``POST /identity/resolve``."""

    def __init__(self, api_base_url: str, api_key: str, client: httpx.Client | None = None) -> None:
        self._url = f"{api_base_url.rstrip('/')}/identity/resolve"
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._client = client or httpx.Client(timeout=_RESOLVE_TIMEOUT)

    def resolve(self, identity_name: str, evidence: dict[str, Any]) -> dict[str, Any] | None:
        """The API's answer, or None when there is none to give. Never raises.

        The tenant is omitted, so the route uses the default one.
        """
        body = {"provider": PROVIDER, "channel_identity": identity_name, "slack": evidence}
        headers = dict(self._headers)
        try:
            inject_trace_context(headers)
            response = self._client.post(self._url, json=body, headers=headers)
            if response.status_code != 200:
                return None
            parsed = response.json()
        except Exception:  # noqa: BLE001 - this seam promises None, never a raise
            # Not only HTTPError: a closed client, a custom transport or a
            # non-JSON body can raise anything, and this seam promises None.
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


def _observe(
    evidence: dict[str, Any],
    client: IdentityResolver,
    log: logging.Logger,
    identity_name: str,
) -> None:
    # Ids only: never message text, and the client never sees a token.
    delivery = evidence.get("delivery")
    user = evidence.get("user_id")
    error: str | None = None
    try:
        answer = client.resolve(identity_name, evidence)
    except Exception as exc:  # noqa: BLE001 - an observation must never raise
        answer, error = None, type(exc).__name__
    if answer is None:
        due, suppressed = _failure_warning_due()
        if due:
            log.warning(
                "slack principal lookup failed identity=%s delivery=%s user=%s error=%s%s",
                identity_name,
                delivery,
                user,
                error or "-",
                f" ({suppressed} similar failures suppressed)" if suppressed else "",
            )
        return
    log.info(
        "slack principal resolution status=%s reason=%s principal_id=%s identity=%s "
        "delivery=%s user=%s",
        answer.get("status"),
        answer.get("reason"),
        answer.get("principal_id") or "-",
        identity_name,
        delivery,
        user,
    )


def observe_principal(
    body: dict[str, Any],
    client: IdentityResolver,
    log: logging.Logger,
    *,
    identity_name: str,
) -> None:
    """Look up and log the sender's principal. Never raises."""
    evidence = slack_evidence(body)
    if evidence is None:
        return
    _observe(evidence, client, log, identity_name)


def principal_observer_middleware(
    client: IdentityResolver,
    *,
    identity_name: str,
    executor: ThreadPoolExecutor | None = None,
    log: logging.Logger = logger,
) -> Callable[..., Any]:
    """A Bolt global middleware that submits the lookup, then calls ``next_``.

    Extracting the evidence is pure dict reads; the lookup itself runs on the
    pool. While ``_MAX_IN_FLIGHT`` lookups are pending the observation is
    skipped, so a slow API can neither queue work without bound nor slow a
    listener. ``identity_name`` is the Slack identity this app serves.
    """

    def _release(_future: Future[None]) -> None:
        # A done-callback fires exactly once, including for a lookup cancelled
        # by shutdown, so a slot is never leaked or released twice.
        _in_flight.release()

    def _middleware(body: dict[str, Any], next_: Callable[[], None]) -> None:
        try:
            evidence = slack_evidence(body)
            if evidence is not None:
                if _in_flight.acquire(blocking=False):
                    # Looked up per submit, so a shutdown's fresh pool is used.
                    pool = executor or _shared_pool()
                    try:
                        future = pool.submit(_observe, evidence, client, log, identity_name)
                    except BaseException:
                        _in_flight.release()
                        raise
                    future.add_done_callback(_release)
                else:
                    log.debug("slack principal lookup skipped: %d in flight", _MAX_IN_FLIGHT)
        except Exception as exc:  # noqa: BLE001 - next() must run whatever happens
            log.warning("slack principal lookup not submitted error=%s", type(exc).__name__)
        next_()

    return _middleware
