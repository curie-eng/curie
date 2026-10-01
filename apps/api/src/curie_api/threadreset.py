"""Force a fresh sandbox for one stuck thread (#713).

A thread's sandbox binds whatever env it booted with for its entire life (model
credential, Slack wiring, bundle version) -- the worker only re-derives env for
a *new* claim, never for one already adopted by a live route. When that env
goes stale (a rotated credential, a bundle redeploy the thread hasn't picked
up, a sandbox wedged after a partial local-stack upgrade), the only way to
force a cold-create today is to reach into Kubernetes/Docker and the Valkey
route key by hand.

This is a lightweight signal, not a pub/sub channel like the kill switch
(``killswitch.py``): a thread-reset request is a one-shot administrative
action with no live "is this still requested" state to gate a running turn on,
so a Valkey SET the worker's existing maintenance tick drains is enough -- no
new subscriber process, no new lifecycle to manage. ``THREAD_RESET_SET`` is
duplicated verbatim in ``apps/worker/src/curie_worker/consumer.py`` (the
worker's own copy), the same cross-service-constant pattern the kill switch
already uses (`apps/worker/src/curie_worker/killswitch.py`'s
``KILL_KEY_PREFIX``/``KILL_CHANNEL`` mirror this module's) since neither
service imports the other's package.

Completion is observed across TWO sets, not one (#812). ``is_pending`` -- which
the CLI's ``reset-thread`` poll gates its "sandbox released" report on -- must
stay True until the release actually lands, so the worker moves a claimed
request into ``THREAD_RESET_INFLIGHT_SET`` for the duration of the release and
clears it only on success. Reading membership of ``THREAD_RESET_SET`` alone
would flip to done the instant the worker SPOPs the request (at CLAIM time),
before -- and independent of whether -- the release actually completed.

The worker also records what a drained reset found (#3699): a key that matches no
route releases nothing, and ``result`` lets the API tell the caller so instead of
leaving a wrong key indistinguishable from a working reset.
"""

import redis.asyncio as redis

# Frozen with the worker and CLI copies in tests/vectors/thread-reset-set.json.
THREAD_RESET_SET = "curie:thread-reset-requests"

# Claimed-but-not-yet-released requests (#812). The worker SPOPs a request off
# ``THREAD_RESET_SET`` (the atomic claim) and moves it here for the duration of
# ``release_thread``, clearing it only once the release actually lands. This
# module reads the UNION of both sets in ``is_pending`` so the observable "reset
# outstanding" signal the CLI polls on flips to done only when the sandbox is
# truly released -- not at claim time, and not at all if the release fails or
# times out (the worker leaves the key here). Duplicated verbatim in
# ``apps/worker/src/curie_worker/consumer.py`` (the worker's own copy), the
# same cross-service-constant pattern as ``THREAD_RESET_SET``.
THREAD_RESET_INFLIGHT_SET = "curie:thread-reset-inflight"

# Outcome of a drained reset (#3699): this prefix plus the thread key holds
# ``released`` when the worker found a route to release and ``no-route`` when the
# key matched none, so nothing was released. The worker writes it before it clears
# the in-progress marker and lets it expire after an hour; ``request`` deletes it
# so a fresh request never reads an earlier reset's outcome. Frozen with the
# worker copy (``apps/worker/src/curie_worker/consumer.py``) in
# tests/vectors/thread-reset-set.json.
THREAD_RESET_RESULT_PREFIX = "curie:thread-reset-result:"


class ThreadResetRequests:
    """Requests (from the API) and drains (from the worker) pending thread
    keys whose sandbox should be force-released on the next maintenance tick."""

    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    async def request(self, thread_key: str) -> None:
        """Queue ``thread_key`` for a forced sandbox release. Idempotent --
        adding an already-pending thread is a no-op (a Valkey SET member).

        Deletes the previous reset's recorded outcome first, so the caller who
        polls after this request can never read the result of an earlier one
        (#3699)."""
        await self._client.delete(f"{THREAD_RESET_RESULT_PREFIX}{thread_key}")
        await self._client.sadd(THREAD_RESET_SET, thread_key)

    async def result(self, thread_key: str) -> str | None:
        """The worker's recorded outcome for the last drained reset of this
        thread, or None when none is recorded (expired, never drained, or a
        worker that predates the record). ``released`` means a route existed and
        was released; ``no-route`` means the key matched no route and nothing was
        released (#3699)."""
        raw = await self._client.get(f"{THREAD_RESET_RESULT_PREFIX}{thread_key}")
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    async def is_pending(self, thread_key: str) -> bool:
        """True while a forced reset for this thread is outstanding: either still
        queued in ``THREAD_RESET_SET``, or claimed by a worker and sitting in
        ``THREAD_RESET_INFLIGHT_SET`` with its ``release_thread`` not yet
        completed (#812). The worker clears the in-progress set only once the
        release SUCCEEDS, so a release that raises or times out keeps this True
        -- the CLI then reports the reset as unconfirmed rather than a false
        "released"."""
        if await self._client.sismember(THREAD_RESET_SET, thread_key):
            return True
        return bool(await self._client.sismember(THREAD_RESET_INFLIGHT_SET, thread_key))
