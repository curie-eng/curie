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
new subscriber process, no new lifecycle to manage. Key names come from the
shared internal keyspace. The ownership protected worker consumer retains its
existing declarations, checked against that canonical keyspace by the vectors.

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
from curie_internal.keyspace import (
    THREAD_RESET_INFLIGHT_SET,
    THREAD_RESET_RESULT_PREFIX,
    THREAD_RESET_SET,
)


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
