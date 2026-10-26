"""Asking the platform whether a Slack caller may start a turn (ADR 0175).

A binding may carry a list of who may talk to the bot through it. The decision
is the API's (`curie_api.admission.admit`), never this process's: the
dispatcher has no database, so it asks ``POST /channels/admission`` with the
platform key, after its own filters and before it claims the event, so a
refused caller never gets a placeholder or a reply.

Asking on every message would put an API round trip in front of every turn,
so answers are cached per route, the binding's kind and address plus the Slack
identity the delivery arrived on (ADR-0168 decision 3):

- A route with no list is cached as OPEN to every caller.
- A route with a list caches each caller's answer separately.
- Every answer also says whether ANY binding on the install carries a list.
  While that install-wide answer is fresh and says "none", no route needs a
  call at all, so an install that never sets a list asks once per TTL in total.
- Answers are fresh for ``CURIE_ADMISSION_CACHE_TTL_SECONDS`` (30 by default),
  which is how long a list change takes to apply in Slack.
- While the API cannot answer, an expired answer still counts until it is
  ``CURIE_ADMISSION_STALE_SECONDS`` old (5 minutes by default), the install-wide
  "no list anywhere" answer included. With nothing usable cached, the caller is
  refused as ``admission_unavailable``.

Answers live in two layers: a bounded in-process LRU in front of Valkey, which
the dispatcher already holds for dedupe. Valkey is what lets a dispatcher that
restarts during an API blip keep answering from what the previous process
learned; every key there expires with the stale window, so a restart can never
serve an answer older than the ADR's bound. Every lookup also checks the
current identity fence in Valkey; pending, missing-uninitializable, malformed
or unavailable fencing forces a fresh API decision. A newer stable generation
partitions both layers, so old identity decisions cannot survive a directory,
account status, identity link or agent policy change.

An API that predates ADR 0175 answers FastAPI's route-miss 404 for the
endpoint. Such an API has no caller lists, so none can refuse anyone: the
answer is "open", logged once per process. This is what keeps a dispatcher
that rolls before its API from dropping Slack traffic.

Concurrent questions about one route share one fetch (single-flight), so a
burst of mentions on an uncached route holds one Bolt listener worker on the
API, not all five. After a fetch fails, nobody asks again for a few seconds
(`_FAILURE_BACKOFF_S`): a hung API costs one client timeout per backoff
window, not one per message.

This module decides nothing about who is on a list. If you find a comparison
of caller ids here, it is a second copy of the API's rule and will drift.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final, Literal
from urllib.parse import urlsplit

import httpx
import redis
from curie_internal.keyspace import ADMISSION_KEY_PREFIX_DEFAULT
from curie_telemetry import inject_trace_context

from .config import DispatcherConfig
from .relevance import DropReason

if TYPE_CHECKING:
    from redis import Redis

logger = logging.getLogger(__name__)

#: The only channel kind this dispatcher serves. Stated, never configured, for
#: the same reason ``handlers._mint_turn`` states it: a Slack dispatcher that
#: could ask about another kind's bindings is a misrouting vector.
SLACK_KIND = "slack"
IDENTITY_FENCE_KEY = "curie:identity:admission-fence"

# Every phase named, because httpx timeouts are per phase and a phase left at
# its default silently joins the sum. The call runs after Bolt has acked the
# envelope, so no Slack deadline applies, but it holds one of Bolt's five
# listener workers for its duration: a platform API that has not answered in
# about three seconds (the four phases summed) is treated as down, and the
# cache's stale window absorbs a short outage.
_ADMISSION_TIMEOUT = httpx.Timeout(connect=0.5, read=1.5, write=0.5, pool=0.5)

# How long a follower waits for the leader of a single-flight fetch: the
# client's worst case plus a margin. Past it the follower answers from the
# cache on its own rather than waiting on a leader that is wedged.
_FOLLOW_TIMEOUT_S = 4.0

# After a fetch fails, how long every route answers from the cache without
# asking the API again. A hung API otherwise costs one full client timeout per
# question, and Bolt has five listener workers: measured against a port that
# accepts and never answers, 20 mentions took 6s of wall time with no backoff
# (four waves of about 1.5s each) whether they were on one route or twenty.
# With it, the first wave pays one timeout and the rest answer from cache.
_FAILURE_BACKOFF_S = 5.0

# The most entries the in-process layer holds before it evicts the least
# recently used. Callers on a restricted route are cached one per caller, and
# every Slack user who mentions the bot is a caller, so without a bound a busy
# shared channel would grow this without limit. Valkey keys expire on their own.
_MAX_ENTRIES = 10_000

# FastAPI's body for a path it has no route for: the one 404 that proves the
# API predates the admission endpoint. Any other 404 (a proxy page, a wrong base
# URL) proves nothing about the API's version and counts as unavailable.
_ROUTE_MISS_BODY = {"detail": "Not Found"}

Clock = Callable[[], float]
RouteKey = tuple[str, str | None, str]
# What a cache lookup can conclude: admit, or a refusal. A lookup that finds
# nothing usable answers None instead, which is NOT a verdict to admit.
_ADMIT: Final = "admit"
Verdict = Literal["admit"] | DropReason


@dataclass(frozen=True)
class AdmissionAnswer:
    """What the API said about one caller on one route.

    Attributes:
        allowed: whether the caller may start a turn.
        restricted: whether the route carries a list at all.
        install_restricted: whether any binding on the install carries one.
            An API that omits it is read as True, the side that fails closed.
    """

    allowed: bool
    restricted: bool
    install_restricted: bool = True
    connect_url: str | None = None


class AdmissionClient:
    """The HTTP half: one ``POST /channels/admission`` per question.

    Holds the platform key (the dispatcher already holds it for approvals) and
    nothing else. Answers None on any failure, a transport error or a non-200
    alike, because the cache treats every one of them the same way: the API
    could not answer, so fall back to a stale answer or refuse. The one
    exception is FastAPI's route-miss 404, which is an answer: see the module
    docstring.
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
        self._skew_logged = False

    def ask(
        self,
        *,
        address: str,
        adapter: str | None,
        callers: tuple[str, ...],
        slack: dict[str, Any] | None = None,
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
        if slack is not None:
            body["slack"] = slack
        headers = dict(self._headers)
        inject_trace_context(headers)
        try:
            response = self._client.post(self._url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning("admission call failed: %s", type(exc).__name__)
            return None
        if response.status_code == 404 and _json_or_none(response) == _ROUTE_MISS_BODY:
            if not self._skew_logged:
                # Once per process: every message on a skewed install would
                # otherwise repeat it, and the fix is one upgrade.
                self._skew_logged = True
                logger.warning(
                    "the platform API predates caller lists (ADR 0175): "
                    "POST /channels/admission is not routed, so every caller is "
                    "admitted until the API is upgraded"
                )
            return AdmissionAnswer(allowed=True, restricted=False, install_restricted=False)
        if response.status_code != 200:
            logger.warning("admission call answered status=%s", response.status_code)
            return None
        parsed = _json_or_none(response)
        answer = _answer_from(parsed)
        if answer is None:
            logger.warning("admission call answered an unreadable body")
        return answer


def _json_or_none(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None


def _answer_from(parsed: Any) -> AdmissionAnswer | None:
    """The answer in a JSON body, or None when a field is missing or mistyped."""

    if not isinstance(parsed, dict):
        return None
    allowed = parsed.get("allowed")
    restricted = parsed.get("restricted")
    install = parsed.get("install_restricted", True)
    if not (
        isinstance(allowed, bool) and isinstance(restricted, bool) and isinstance(install, bool)
    ):
        return None
    connect_url = parsed.get("connect_url")
    if connect_url is not None:
        if not isinstance(connect_url, str) or len(connect_url) > 4096:
            return None
        try:
            url = urlsplit(connect_url)
            if (
                url.scheme not in {"http", "https"}
                or not url.hostname
                or url.username
                or url.password
            ):
                return None
        except ValueError:
            return None
    return AdmissionAnswer(
        allowed=allowed, restricted=restricted, install_restricted=install, connect_url=connect_url
    )


@dataclass
class _Flight:
    """One in-flight fetch for a route, which later askers wait on."""

    done: threading.Event = field(default_factory=threading.Event)
    failed: bool = False


class AdmissionGate:
    """The cached admission check every turn-starting Slack lane calls.

    One instance per process, shared by every identity's Bolt app: the route
    key carries the identity, so two identities never share an answer. Bolt
    runs listeners on a thread pool, so the in-process layer is guarded by a
    lock; the HTTP call itself runs outside it.
    """

    def __init__(
        self,
        client: AdmissionClient,
        *,
        ttl_s: float,
        stale_s: float,
        redis_client: Redis | None,
        key_prefix: str = ADMISSION_KEY_PREFIX_DEFAULT,
        clock: Clock = time.time,
        max_entries: int = _MAX_ENTRIES,
    ) -> None:
        self._client = client
        self._ttl_s = ttl_s
        self._stale_s = stale_s
        self._redis = redis_client
        self._prefix = key_prefix
        # Wall clock, not monotonic: an answer's age must mean the same thing
        # to the next process that reads it back out of Valkey.
        self._clock = clock
        self._max_entries = max_entries
        self._lock = threading.Lock()
        # key -> (answer fields, fetched_at). The in-process layer.
        self._entries: OrderedDict[str, tuple[dict[str, bool], float]] = OrderedDict()
        self._flights: dict[RouteKey, _Flight] = {}
        # When the last fetch failed, on this gate's clock; None when the last
        # fetch answered. Inside `_FAILURE_BACKOFF_S` of it nobody asks.
        self._failed_at: float | None = None
        self._failed_generation: str | None = None

    def refusal(
        self,
        *,
        address: str,
        adapter: str | None,
        callers: Iterable[str],
        slack: dict[str, Any] | None = None,
        on_connect: Callable[[str], None] | None = None,
    ) -> DropReason | None:
        """Use cached authority only under a currently verified stable fence.

        A connect URL is delivered only to the requesting handler from its own
        fresh HTTP answer. Neither cache layer nor a single-flight follower
        can recover or share that single-use channel-account credential.
        """
        route: RouteKey = (SLACK_KIND, adapter, address)
        ids = tuple(sorted({caller for caller in callers if caller}))
        generation = self._generation()
        if generation is None:
            return self._fresh_refusal(address, adapter, ids, slack, on_connect)
        keys = self._keys(route, ids, generation, slack)
        cached = self._verdict_from_cache(keys, self._ttl_s)
        if cached is not None:
            if self._generation() == generation:
                return _final(cached)
            return self._fresh_refusal(address, adapter, ids, slack, on_connect)
        if self._backing_off(generation):
            if self._generation() == generation:
                return self._stale_verdict(keys)
            return self._fresh_refusal(address, adapter, ids, slack, on_connect)

        # A follower shares only a decision for the same generation, caller
        # and canonical signed-transport evidence, never a connect URL.
        flight_key: RouteKey = (SLACK_KIND, adapter, keys["caller"])
        flight, leader = self._join_flight(flight_key)
        if not leader:
            flight.done.wait(_FOLLOW_TIMEOUT_S)
            cached = self._verdict_from_cache(keys, self._ttl_s)
            if cached is not None and self._generation() == generation:
                return _final(cached)
            if flight.failed:
                return (
                    self._stale_verdict(keys)
                    if self._generation() == generation
                    else DropReason.ADMISSION_UNAVAILABLE
                )
        try:
            answer = self._client.ask(address=address, adapter=adapter, callers=ids, slack=slack)
        except BaseException:
            if leader:
                self._land_flight(flight_key, flight, failed=True)
            raise
        with self._lock:
            self._failed_at = None if answer is not None else self._clock()
            self._failed_generation = generation
        # A writer can commit while the HTTP request is in flight. Its new
        # generation must never receive a decision made against older state.
        unchanged = self._generation() == generation
        if answer is not None and unchanged:
            self._store(keys, answer)
        if leader:
            self._land_flight(flight_key, flight, failed=answer is None)
        if answer is None:
            return self._stale_verdict(keys) if unchanged else DropReason.ADMISSION_UNAVAILABLE
        return self._answer_refusal(answer, on_connect)

    def _fresh_refusal(
        self,
        address: str,
        adapter: str | None,
        ids: tuple[str, ...],
        slack: dict[str, Any] | None,
        on_connect: Callable[[str], None] | None,
    ) -> DropReason | None:
        answer = self._client.ask(address=address, adapter=adapter, callers=ids, slack=slack)
        if answer is None:
            return DropReason.ADMISSION_UNAVAILABLE
        return self._answer_refusal(answer, on_connect)

    @staticmethod
    def _answer_refusal(
        answer: AdmissionAnswer,
        on_connect: Callable[[str], None] | None,
    ) -> DropReason | None:
        if not answer.allowed and answer.connect_url is not None and on_connect is not None:
            on_connect(answer.connect_url)
        return None if answer.allowed else DropReason.CALLER_NOT_ALLOWED

    def _generation(self) -> str | None:
        """None means pending, malformed or unavailable: fresh API only.

        A missing key is initialized with SET NX and reread. The unique
        generation preserves legacy caching without resurrecting old L1 allows
        after a Valkey flush or lost fence key.
        """
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(IDENTITY_FENCE_KEY)
        except redis.RedisError:
            return None
        if raw is None:
            try:
                self._redis.set(IDENTITY_FENCE_KEY, "stable:" + uuid.uuid4().hex, nx=True)
                raw = self._redis.get(IDENTITY_FENCE_KEY)
            except redis.RedisError:
                return None
        try:
            value = raw.decode("ascii") if isinstance(raw, bytes) else raw
            if not isinstance(value, str) or not value.startswith("stable:"):
                return None
            uuid.UUID(value.removeprefix("stable:"))
            return value
        except (ValueError, UnicodeError, AttributeError):
            return None

    # --- single-flight and backoff ---------------------------------------------

    def _backing_off(self, generation: str) -> bool:
        with self._lock:
            failed_at = self._failed_at
        return (
            self._failed_generation == generation
            and failed_at is not None
            and self._clock() - failed_at < _FAILURE_BACKOFF_S
        )

    def _join_flight(self, route: RouteKey) -> tuple[_Flight, bool]:
        with self._lock:
            flight = self._flights.get(route)
            if flight is not None:
                return flight, False
            flight = _Flight()
            self._flights[route] = flight
            return flight, True

    def _land_flight(self, route: RouteKey, flight: _Flight, *, failed: bool) -> None:
        flight.failed = failed
        with self._lock:
            if self._flights.get(route) is flight:
                del self._flights[route]
        flight.done.set()

    # --- the cache -------------------------------------------------------------

    def _keys(
        self, route: RouteKey, ids: tuple[str, ...], generation: str, slack: dict[str, Any] | None
    ) -> dict[str, str]:
        kind, adapter, address = route
        evidence = hashlib.sha256(
            json.dumps(slack, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:32]
        partition = f"{generation}:{evidence}"
        route_part = f"{partition}:{kind}:{adapter or ''}:{address}"
        digest = hashlib.sha256("\n".join(ids).encode()).hexdigest()[:32]
        return {
            "open": f"{self._prefix}open:{route_part}",
            "caller": f"{self._prefix}caller:{route_part}:{digest}",
            "install": f"{self._prefix}install:{partition}",
        }

    def _verdict_from_cache(self, keys: dict[str, str], max_age_s: float) -> Verdict | None:
        """A verdict from an answer no older than ``max_age_s``, or None.

        The route's open entry first, then this caller's own answer, then the
        install-wide "no binding carries a list" answer. None means "nothing
        usable cached", which is not a verdict to admit.
        """

        if self._read(keys["open"], max_age_s) is not None:
            return _ADMIT
        caller = self._read(keys["caller"], max_age_s)
        if caller is not None:
            return _ADMIT if caller["allowed"] else DropReason.CALLER_NOT_ALLOWED
        install = self._read(keys["install"], max_age_s)
        if install is not None and not install["restricted"]:
            return _ADMIT
        return None

    def _stale_verdict(self, keys: dict[str, str]) -> DropReason | None:
        verdict = self._verdict_from_cache(keys, self._stale_s)
        if verdict is None:
            return DropReason.ADMISSION_UNAVAILABLE
        return _final(verdict)

    def _read(self, key: str, max_age_s: float) -> dict[str, bool] | None:
        now = self._clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                self._entries.move_to_end(key)
        if entry is None:
            entry = self._read_valkey(key)
            if entry is not None:
                self._remember(key, entry)
        if entry is None:
            return None
        fields, fetched_at = entry
        return fields if max(0.0, now - fetched_at) < max_age_s else None

    def _store(self, keys: dict[str, str], answer: AdmissionAnswer) -> None:
        """Record a fresh answer under the keys it describes.

        An unrestricted answer describes the whole route, so it is stored as
        the route's open entry. A restricted answer describes only this caller,
        and it also removes any open entry the route still had: the route has
        gained a list, and an older open answer must not keep admitting
        callers the list refuses once this call has seen the list. Every answer
        also refreshes the install-wide entry.
        """

        now = self._clock()
        install = ({"restricted": answer.install_restricted}, now)
        if answer.restricted:
            self._forget(keys["open"])
            self._write(keys["caller"], ({"allowed": answer.allowed}, now))
        else:
            self._write(keys["open"], ({"allowed": True}, now))
        self._write(keys["install"], install)

    def _write(self, key: str, entry: tuple[dict[str, bool], float]) -> None:
        self._remember(key, entry)
        if self._redis is None:
            return
        fields, fetched_at = entry
        try:
            self._redis.set(
                key,
                json.dumps({**fields, "at": fetched_at}),
                # Nothing outlives the stale window, so no restart can serve an
                # answer older than ADR 0175 allows.
                ex=max(1, int(self._stale_s)),
            )
        except redis.RedisError as exc:
            logger.debug("admission cache write skipped: %s", type(exc).__name__)

    def _forget(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)
        if self._redis is None:
            return
        try:
            self._redis.delete(key)
        except redis.RedisError as exc:
            logger.debug("admission cache delete skipped: %s", type(exc).__name__)

    def _remember(self, key: str, entry: tuple[dict[str, bool], float]) -> None:
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def _read_valkey(self, key: str) -> tuple[dict[str, bool], float] | None:
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(key)
        except redis.RedisError as exc:
            logger.debug("admission cache read skipped: %s", type(exc).__name__)
            return None
        if raw is None:
            return None
        try:
            parsed = json.loads(raw)
            fetched_at = float(parsed.pop("at"))
        except (TypeError, ValueError, KeyError, AttributeError):
            return None
        fields = {name: value for name, value in parsed.items() if isinstance(value, bool)}
        return fields, fetched_at


def _final(verdict: Verdict) -> DropReason | None:
    """A cache verdict as the caller-facing answer: None admits."""
    return verdict if isinstance(verdict, DropReason) else None


def build_admission(config: DispatcherConfig, redis_client: Redis | None) -> AdmissionGate:
    """The production gate, from the dispatcher's API, cache and Valkey settings."""

    return AdmissionGate(
        AdmissionClient(api_base_url=config.api_base_url, api_key=config.api_key),
        ttl_s=config.admission_cache_ttl_s,
        stale_s=config.admission_stale_s,
        redis_client=redis_client,
        key_prefix=config.admission_cache_prefix,
    )
