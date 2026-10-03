"""The end to end namespace reaper (#3245, ADR 0176 decision 4).

Teardown must not depend on the agent calling env_destroy. An agent that
crashes, runs out of budget, or simply forgets leaves a namespace holding
quota on the test cluster. This loop deletes every namespace the installation
owns once its TTL has passed or its run has reached a terminal status.

Why the worker hosts it rather than the connector pod or the API:

**The worker already knows run state.** The run label is the ExecutionRequest
id, and the worker reads requests through the internal work-item endpoint with
the token it already holds. The connector pod has the kubeconfig but no way to
learn whether a run finished, and it disappears when its agent is undeployed,
leaking that agent's namespaces.

**The worker can already read the kubeconfig.** It lives in each agent's
``{release}-{agent}-connector-secrets`` Secret, and the connector reconciler's
Role grants the worker ``list`` on Secrets. No new RBAC and no new copy of the
credential; the API would need one mounted.

The Kubernetes-shaped sweep lives in ``curie_e2e_connector.reaper`` next to
env_create. This module supplies the clusters, the run statuses and the
health signal. The cluster calls are synchronous, so they run in a worker
thread rather than blocking the event loop the kernel is using.

The health signal is three gauges. ``curie.e2e.reaper.last_success`` is the
unix time of the last clean pass, recorded every pass and 0 until one
succeeds, so a reaper that never worked alerts the same as one that stopped.
The OTel SDK's synchronous gauge forgets its value after each collection, so
a slow pass would make the series vanish rather than go stale. While a pass
runs, the loop records the unchanged timestamp every interval, so a live
reaper task always reports and only a dead one goes absent. A slow pass is
never cancelled or overlapped: cancelling would leave its worker threads
blocked on the shared default executor and restart it from the beginning, so
during a slow API outage expired namespaces would never reach deletion. Past
``slow_pass_s`` it only logs a warning.
``curie.e2e.namespaces.expired`` counts scoped namespaces past their TTL, and
``curie.e2e.namespaces.overdue`` the ones at least ten minutes past it or
without one, which is what the page keys on.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import logging
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from functools import partial
from typing import Any, Protocol

from curie_e2e_connector.contract import KUBECONFIG_SECRET
from curie_e2e_connector.kube import (
    ClusterApi,
    ClusterError,
    HttpxCluster,
    client_from_kubeconfig_text,
)
from curie_e2e_connector.reaper import (
    Scope,
    ScopedNamespace,
    is_expired,
    scoped_namespaces,
    sweep,
)
from curie_telemetry import record_metric

from .workitem_dispatch import WorkItemConflict

logger = logging.getLogger(__name__)

# The ExecutionRequest statuses after which nothing will use the namespace again.
TERMINAL_STATUSES = frozenset({"completed", "failed", "expired", "cancelled"})

_ATTRIBUTES = {"service.name": "curie-worker"}
_LAST_SUCCESS = "curie.e2e.reaper.last_success"
_EXPIRED = "curie.e2e.namespaces.expired"
_OVERDUE = "curie.e2e.namespaces.overdue"
_CONNECTOR_SECRET_SUFFIX = "-connector-secrets"

RequestStatus = Callable[[uuid.UUID], Awaitable[str | None]]
ClusterSource = Callable[[], Sequence[ClusterApi]]


class _RequestReader(Protocol):
    async def get_request(self, request_id: uuid.UUID) -> Any: ...


def request_status_lookup(client: _RequestReader) -> RequestStatus:
    """Map a run id to its request status, or None when the API has no such request.

    An unknown run is left to its TTL. A transport error propagates: the loop
    treats that run as live for this pass and fails the pass.
    """

    async def lookup(request_id: uuid.UUID) -> str | None:
        try:
            view = await client.get_request(request_id)
        except WorkItemConflict as exc:
            if exc.code == "not_found":
                return None
            raise
        status = getattr(view, "status", None)
        return str(status) if status is not None else None

    return lookup


def connector_secret_clusters(
    core: Any, *, namespace: str, release: str, timeout: float
) -> list[ClusterApi]:
    """One client per distinct test cluster kubeconfig in this release's connector Secrets.

    Two agents sharing a kubeconfig share a client, so a cluster is swept once.
    A kubeconfig that is not base64 UTF-8, or that the connector would refuse,
    becomes a refused entry whose every request raises ``ClusterError`` naming
    the Secret and the reason, so the pass fails rather than silently sweeping
    nothing. Its contents are never logged or raised.
    """

    prefix = f"{release}-"
    secrets = core.list_namespaced_secret(namespace, _request_timeout=timeout)
    seen: set[str] = set()
    clusters: list[ClusterApi] = []
    for secret in secrets.items or []:
        name = getattr(secret.metadata, "name", None) or ""
        if not (
            name.startswith(prefix)
            and name.endswith(_CONNECTOR_SECRET_SUFFIX)
            and len(name) > len(prefix) + len(_CONNECTOR_SECRET_SUFFIX)
        ):
            continue
        encoded = (secret.data or {}).get(KUBECONFIG_SECRET)
        if not encoded:
            continue
        try:
            text = base64.b64decode(encoded, validate=True).decode("utf-8")
        except (binascii.Error, ValueError):
            clusters.append(_RefusedCluster(name, f"{KUBECONFIG_SECRET} is not base64 UTF-8"))
            continue
        if text in seen:
            continue
        seen.add(text)
        try:
            client = client_from_kubeconfig_text(text, timeout=timeout)
        except ClusterError as exc:
            clusters.append(_RefusedCluster(name, str(exc)))
            continue
        clusters.append(HttpxCluster(client))
    return clusters


class _RefusedCluster(ClusterApi):
    """A connector Secret whose kubeconfig cannot be used. Every request fails."""

    def __init__(self, secret: str, reason: str) -> None:
        self._message = f"secret={secret} has an unusable kubeconfig: {reason}"

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        raise ClusterError(self._message)


def _now() -> datetime:
    return datetime.now(UTC)


class E2EReaperLoop:
    def __init__(
        self,
        *,
        clusters: ClusterSource,
        request_status: RequestStatus,
        scope: Scope,
        interval_s: float,
        wall_clock: Callable[[], datetime] = _now,
        slow_pass_s: float | None = None,
    ) -> None:
        self._clusters = clusters
        self._request_status = request_status
        self._scope = scope
        self._interval = interval_s
        self._wall_clock = wall_clock
        self._slow_pass = slow_pass_s if slow_pass_s is not None else max(interval_s * 5, 300.0)
        self._last_success = 0.0

    async def sweep_once(self) -> bool:
        """One pass over every test cluster. True when the pass was clean.

        Clean means every cluster listed, every status lookup answered, and
        every namespace due for deletion was deleted. A failure in one cluster
        or one namespace does not stop the rest of the pass.
        """

        now = self._wall_clock()
        ok = True
        listed_all = True
        expired = 0
        overdue = 0
        statuses: dict[uuid.UUID, str | None] = {}
        failed_lookups: set[uuid.UUID] = set()
        try:
            clusters = await asyncio.to_thread(self._clusters)
        except Exception:
            logger.exception("e2e reaper could not read the connector Secrets")
            clusters = []
            ok = listed_all = False
        for cluster in clusters:
            try:
                found = await asyncio.to_thread(scoped_namespaces, cluster, self._scope)
            except ClusterError as exc:
                logger.warning("e2e reaper could not list namespaces: %s", exc)
                ok = listed_all = False
                continue
            terminal = await self._terminal_runs(found, statuses, failed_lookups)
            result = await asyncio.to_thread(
                partial(sweep, cluster, self._scope, found, now=now, terminal_runs=terminal)
            )
            expired += result.expired
            overdue += result.overdue
            by_name = {namespace.name: namespace for namespace in found}
            for name in result.reaped:
                reason = "ttl" if is_expired(by_name[name], now) else "terminal"
                logger.warning("e2e reaper deleted namespace=%s reason=%s", name, reason)
            if result.failed:
                ok = False
        if failed_lookups:
            ok = False
        if ok:
            self._last_success = now.timestamp()
        self._record_last_success()
        # A cluster that did not list contributes nothing, so the counts would
        # read low. Hold the last full counts instead; the stalled alert covers
        # a reaper that cannot list at all.
        if listed_all:
            record_metric(_EXPIRED, expired, attributes=_ATTRIBUTES)
            record_metric(_OVERDUE, overdue, attributes=_ATTRIBUTES)
        return ok

    def _record_last_success(self) -> None:
        record_metric(_LAST_SUCCESS, self._last_success, attributes=_ATTRIBUTES)

    async def _terminal_runs(
        self,
        found: list[ScopedNamespace],
        statuses: dict[uuid.UUID, str | None],
        failed: set[uuid.UUID],
    ) -> set[str]:
        """The run labels among ``found`` whose request is terminal.

        Each run is looked up once per pass, across clusters. A label that is
        not a uuid is never looked up, so only its TTL applies.
        """

        terminal: set[str] = set()
        for namespace in found:
            run = _run_id(namespace.run)
            if run is None:
                continue
            if run not in statuses and run not in failed:
                try:
                    statuses[run] = await self._request_status(run)
                except Exception as exc:
                    logger.warning(
                        "e2e reaper could not read request=%s status: %s; "
                        "keeping its namespace this pass",
                        run,
                        exc,
                    )
                    failed.add(run)
                    continue
            if statuses.get(run) in TERMINAL_STATUSES:
                terminal.add(namespace.run)
        return terminal

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        """Sweep on an interval until told to stop. Never raises.

        One pass at a time, each run to completion: the next starts an
        interval after the previous one finished. Only a stop cancels a pass.
        """

        stop = stop or asyncio.Event()
        logger.info(
            "e2e reaper started prefix=%s interval=%ss",
            self._scope.namespace_prefix,
            self._interval,
        )
        stopping = asyncio.create_task(stop.wait())
        try:
            while not stop.is_set():
                await self._run_pass(stopping)
                if stop.is_set():
                    break
                await asyncio.wait({stopping}, timeout=self._interval)
        finally:
            stopping.cancel()
        logger.info("e2e reaper stopped")

    async def _run_pass(self, stopping: asyncio.Task[Any]) -> None:
        """Run one pass to completion, reporting the stale timestamp meanwhile."""

        sweeping = asyncio.create_task(self.sweep_once())
        started = asyncio.get_running_loop().time()
        warned = False
        try:
            while not sweeping.done():
                await asyncio.wait(
                    {sweeping, stopping},
                    timeout=self._interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if sweeping.done():
                    break
                if stopping.done():
                    sweeping.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await sweeping
                    return
                # Keep the gauge reported, stale rather than absent.
                self._record_last_success()
                elapsed = asyncio.get_running_loop().time() - started
                if not warned and elapsed > self._slow_pass:
                    logger.warning(
                        "e2e reaper pass has run %.0fs, past %ss; letting it finish",
                        elapsed,
                        self._slow_pass,
                    )
                    warned = True
        except asyncio.CancelledError:
            sweeping.cancel()
            raise
        if sweeping.cancelled():
            logger.warning("e2e reaper pass was cancelled; retrying next interval")
        elif (exc := sweeping.exception()) is not None:
            logger.error("e2e reaper pass failed; retrying next interval", exc_info=exc)


def _run_id(label: str) -> uuid.UUID | None:
    try:
        run = uuid.UUID(label)
    except ValueError:
        return None
    # Only the canonical spelling env_create writes; anything else is not a run id.
    return run if str(run) == label else None
