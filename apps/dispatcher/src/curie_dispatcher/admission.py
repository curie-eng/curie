"""Asking the platform whether a Slack caller may start a turn (ADR 0175).

A binding may carry a list of who may talk to the bot through it. The decision
is the API's (`curie_api.admission.admit`), never this process's: the
dispatcher has no database, so it asks ``POST /channels/admission`` with the
platform key, after its own filters and before it claims the event, so a
refused caller never gets a placeholder or a reply.

Asking on every message would put an API round trip in front of every turn,
so answers are cached per route, the binding's kind and address plus the Slack
identity the delivery arrived on (ADR-0168 decision 3):

- A route with no list is cached as OPEN to every caller, so an install that
  never sets a list makes one call per route per TTL, not one per message.
- A route with a list caches each caller's answer separately.
- Answers are fresh for ``CURIE_ADMISSION_CACHE_TTL_SECONDS`` (30 by default),
  which is how long a list change takes to apply in Slack.
- While the API cannot answer, an expired answer still counts until it is
  ``CURIE_ADMISSION_STALE_SECONDS`` old (5 minutes by default). With nothing
  usable cached, the caller is refused as ``admission_unavailable``: failing
  closed means first contact on an uncached route is refused during an outage,
  even on a route with no list.

This module decides nothing about who is on a list. If you find a comparison
of caller ids here, it is a second copy of the API's rule and will drift.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass

import httpx
from curie_telemetry import inject_trace_context

from .config import DispatcherConfig
from .relevance import DropReason

logger = logging.getLogger(__name__)

#: The only channel kind this dispatcher serves. Stated, never configured, for
#: the same reason ``handlers._mint_turn`` states it: a Slack dispatcher that
#: could ask about another kind's bindings is a misrouting vector.
SLACK_KIND = "slack"

# Every phase named, because httpx timeouts are per phase and a phase left at
# its default silently joins the sum. The call runs after Bolt has acked the
# envelope, so no Slack deadline applies, but it holds one of Bolt's five
# listener workers for its duration: a platform API that has not answered in
# about two seconds is treated as down, and the cache's stale window absorbs a
# short outage.
_ADMISSION_TIMEOUT = httpx.Timeout(connect=0.5, read=1.5, write=0.5, pool=0.5)

# The most entries the cache holds before it evicts the least recently used.
# Callers on a restricted route are cached one per caller, and every Slack user
# who mentions the bot is a caller, so without a bound a busy shared channel
# would grow this without limit.
_MAX_ENTRIES = 10_000

Clock = Callable[[], float]
RouteKey = tuple[str, str | None, str]
CacheKey = tuple[RouteKey, tuple[str, ...] | None]


@dataclass(frozen=True)
class AdmissionAnswer:
    """What the API said about one caller on one route.

    Attributes:
        allowed: whether the caller may start a turn.
        restricted: whether the route carries a list at all.
    """

    allowed: bool
    restricted: bool


class AdmissionClient:
    """The HTTP half: one ``POST /channels/admission`` per question.

    Holds the platform key (the dispatcher already holds it for approvals) and
    nothing else. Answers None on ANY failure, a transport error or a non-200
    alike, because the cache treats every one of them the same way: the API
    could not answer, so fall back to a stale answer or refuse.
    """

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        client: httpx.Client | None = None,
    ) -> None:
        self._url = f"{api_base_url.rstrip('/')}/channels/admission"
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._client = client or httpx.Client(timeout=_ADMISSION_TIMEOUT)

    def ask(
        self, *, address: str, adapter: str | None, callers: tuple[str, ...]
    ) -> AdmissionAnswer | None:
        """Ask whether ``callers`` may start a turn on the Slack route.

        Args:
            address: the Slack conversation id the delivery arrived in.
            adapter: the identity half of the route, None for the default app.
            callers: every id Slack reports for the caller.

        Returns:
            The API's answer, or None when it could not be obtained.
        """

        body: dict[str, object] = {
            "kind": SLACK_KIND,
            "address": address,
            "callers": list(callers),
        }
        if adapter is not None:
            body["adapter"] = adapter
        headers = dict(self._headers)
        inject_trace_context(headers)
        try:
            response = self._client.post(self._url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning("admission call failed: %s", type(exc).__name__)
            return None
        if response.status_code != 200:
            logger.warning("admission call answered status=%s", response.status_code)
            return None
        try:
            parsed = response.json()
            allowed = parsed["allowed"]
            restricted = parsed["restricted"]
        except (ValueError, KeyError, TypeError):
            logger.warning("admission call answered an unreadable body")
            return None
        if not isinstance(allowed, bool) or not isinstance(restricted, bool):
            logger.warning("admission call answered an unreadable body")
            return None
        return AdmissionAnswer(allowed=allowed, restricted=restricted)


class AdmissionGate:
    """The cached admission check every turn-starting Slack lane calls.

    One instance per process, shared by every identity's Bolt app: the route
    key carries the identity, so two identities never share an answer. Bolt
    runs listeners on a thread pool, so the cache is guarded by a lock; the
    HTTP call itself runs outside it, so one slow answer does not stall a
    delivery on another route.
    """

    def __init__(
        self,
        client: AdmissionClient,
        *,
        ttl_s: float,
        stale_s: float,
        clock: Clock = time.monotonic,
        max_entries: int = _MAX_ENTRIES,
    ) -> None:
        self._client = client
        self._ttl_s = ttl_s
        self._stale_s = stale_s
        self._clock = clock
        self._max_entries = max_entries
        self._lock = threading.Lock()
        # key -> (answer, fetched_at). A route key with no caller part is the
        # route's OPEN entry; a key with a caller part is one caller's answer
        # on a restricted route.
        self._entries: OrderedDict[CacheKey, tuple[AdmissionAnswer, float]] = OrderedDict()

    def refusal(
        self, *, address: str, adapter: str | None, callers: Iterable[str]
    ) -> DropReason | None:
        """The reason this caller must not start a turn, or None to admit.

        Args:
            address: the Slack conversation id the delivery arrived in.
            adapter: the identity half of the route, None for the default app.
            callers: every id Slack reports for the caller; blanks are dropped.

        Returns:
            None to admit, ``CALLER_NOT_ALLOWED`` when the list refuses the
            caller, or ``ADMISSION_UNAVAILABLE`` when the API could not answer
            and nothing usable was cached.
        """

        route: RouteKey = (SLACK_KIND, adapter, address)
        ids = tuple(sorted({caller for caller in callers if caller}))
        open_key: CacheKey = (route, None)
        caller_key: CacheKey = (route, ids)

        cached = self._lookup(open_key, caller_key, self._ttl_s)
        if cached is not None:
            return self._verdict(cached)

        answer = self._client.ask(address=address, adapter=adapter, callers=ids)
        if answer is not None:
            self._store(open_key, caller_key, answer)
            return self._verdict(answer)

        stale = self._lookup(open_key, caller_key, self._stale_s)
        if stale is not None:
            return self._verdict(stale)
        return DropReason.ADMISSION_UNAVAILABLE

    @staticmethod
    def _verdict(answer: AdmissionAnswer) -> DropReason | None:
        return None if answer.allowed else DropReason.CALLER_NOT_ALLOWED

    def _lookup(
        self, open_key: CacheKey, caller_key: CacheKey, max_age_s: float
    ) -> AdmissionAnswer | None:
        """A cached answer no older than ``max_age_s``: the route's open entry
        first, then this caller's own."""

        now = self._clock()
        with self._lock:
            for key in (open_key, caller_key):
                entry = self._entries.get(key)
                if entry is not None and now - entry[1] < max_age_s:
                    self._entries.move_to_end(key)
                    return entry[0]
        return None

    def _store(self, open_key: CacheKey, caller_key: CacheKey, answer: AdmissionAnswer) -> None:
        """Record a fresh answer under the key it describes.

        An unrestricted answer describes the whole route, so it is stored as
        the route's open entry. A restricted answer describes only this caller,
        and it also removes any open entry the route still had: the route has
        gained a list, and an older open answer must not keep admitting
        callers the list refuses once this call has seen the list.
        """

        now = self._clock()
        with self._lock:
            if answer.restricted:
                self._entries.pop(open_key, None)
                self._entries[caller_key] = (answer, now)
                self._entries.move_to_end(caller_key)
            else:
                self._entries[open_key] = (answer, now)
                self._entries.move_to_end(open_key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


def build_admission(config: DispatcherConfig) -> AdmissionGate:
    """The production gate, from the dispatcher's API and cache settings."""

    return AdmissionGate(
        AdmissionClient(api_base_url=config.api_base_url, api_key=config.api_key),
        ttl_s=config.admission_cache_ttl_s,
        stale_s=config.admission_stale_s,
    )
