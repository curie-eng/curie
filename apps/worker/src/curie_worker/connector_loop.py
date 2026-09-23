"""The connector reconcile loop (ADR-0090, #1184).

Runs in the worker, which already holds cluster-write authority for
SandboxClaims and already reads the agents tables. The alternative -- its own
Deployment -- would need its own ServiceAccount, database credential, API key
and image for what is a periodic query plus a handful of applies.

Three things make this safe to run unattended, and each is a deliberate choice
rather than a default:

**One agent's failure ends with that agent.** A pass reconciles every agent with
an active deployment. An exception in one is caught and logged. Recognized
platform 5xx responses have a bounded quiet retry window before failed
accounting and traceback escalation; other exceptions are counted immediately.
The rest of the pass continues. A reconciler that aborts the sweep on the first
bad agent leaves every later agent unreconciled indefinitely, and the ordering
is arbitrary, so which agents those are changes between passes.

**A pass never kills the worker.** The loop's body is wrapped whole. This shares
a process with the kernel, whose four correctness rules are not negotiable, so
the loop is a background task that can fail loudly and repeatedly without taking
anything else down with it.

**Quiet when converged.** The steady state is "nothing changed", which is most
passes forever. Only work and failures are logged at INFO; a converged pass
logs at DEBUG. A loop that narrates every pass trains people to ignore it, and
then the one pass that mattered scrolls by unread.

The cluster-shaped work is synchronous -- the Kubernetes client is, and so is
the render fetch -- so a pass runs in a worker thread rather than blocking the
event loop the kernel is using.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from curie_telemetry import record_metric
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .connector_agent import (
    AgentOutcome,
    ManifestSource,
    RenderedConnectors,
    prune_agent,
    reconcile_agent,
)
from .connector_apply import ConnectorClient

logger = logging.getLogger(__name__)
_monotonic = time.monotonic

_PLATFORM_5XX_GRACE_PASSES = 3


# Every agent with an in-force deployment, and the version that deployment
# points at. RANK FIRST, DECIDE SECOND: this must select the SAME winner
# binding.py's _RESOLVE_SQL does -- prod outranks dev, then most recent
# deployed_at -- because the connectors a thread gets have to belong to the
# version its sandbox actually boots. DISTINCT ON collapses an agent with both
# active to that one winner; without it an agent would be reconciled twice per
# pass, the second undoing the first.
#
# The bundle is REPORTED (has_bundle), never filtered on (#1216). The predicate
# used to read `WHERE v.bundle_ref IS NOT NULL`, which filtered before ranking:
# when the true winner was a bundleless version, the row vanished and the
# NEXT-ranked version -- a lower-precedence one the sandbox is not booting --
# was silently promoted and its connector objects applied, with nothing to
# converge them away. Filtering here can only ever disagree with binding; a
# version we cannot render for is a fact about what this pass may DO, not about
# which version is in force, so it is decided in _reconcile_one instead.
#
# The trailing `d.id DESC` carries no meaning of its own -- id order is not a
# precedence rule and nothing may start reading one into it. It exists only to
# make the order TOTAL: two active deployments in the same environment with an
# identical `deployed_at` leave the first two keys tied, and an undefined tie
# lets this query and binding.py's _RESOLVE_SQL -- different joins, different
# plans -- pick different winners for the same agent. Since the prune path
# above, that disagreement costs a DESTRUCTIVE prune (this loop deletes the
# connector objects of the version the sandbox is actually booting), not merely
# a stale apply, so the key is duplicated verbatim in both statements.
_TARGETS_SQL = """
SELECT DISTINCT ON (a.id)
       a.id AS agent_id,
       a.name AS agent_name,
       v.id AS version_id,
       v.bundle_ref IS NOT NULL AS has_bundle
FROM {schema}.agents a
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
ORDER BY a.id, (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""


@dataclass(frozen=True)
class AgentTarget:
    agent_id: uuid.UUID
    agent_name: str
    version_id: uuid.UUID
    # Whether the in-force version has a stored bundle. False means the render
    # endpoint has nothing to serve, so this agent gets the prune-only path
    # rather than a reconcile (#1216). Defaults True so a caller constructing a
    # target for the ordinary case says nothing about a condition that is the
    # exception.
    has_bundle: bool = True


class HttpManifestSource:
    """Fetches rendered connector objects from the platform API.

    Synchronous on purpose: its caller runs in a worker thread, and the
    Kubernetes client beside it is sync too. Rendering stays in the API by
    ADR-0090 -- re-deriving the objects here would recreate the drift
    `connectors.yaml` exists to prevent.
    """

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        release: str,
        namespace: str,
        app_name: str,
        timeout: float = 30.0,
    ) -> None:
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._params = {"release": release, "namespace": namespace, "app_name": app_name}
        self._timeout = timeout

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        url = f"{self._base}/agents/{agent_id}/versions/{version_id}/connectors"
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(url, params=self._params, headers=self._headers)
        response.raise_for_status()
        body: dict[str, Any] = response.json()
        return RenderedConnectors(
            manifests=list(body.get("manifests") or []),
            owned_secret_name=str(body.get("owned_secret_name") or ""),
            owned_secret_keys=list(body.get("owned_secret_keys") or []),
        )


@dataclass
class PassSummary:
    """What one sweep did. Reported so a pass is auditable without a debugger.

    Counters are per-event, not a partition of agents -- a skipped agent whose
    delete-only prune still ran can add to both ``skipped`` and ``deleted``,
    or to both ``skipped`` and ``failed`` if that prune errored. Do not expect
    them to sum to ``reconciled``.
    """

    reconciled: int = 0
    applied: int = 0
    deleted: int = 0
    skipped: int = 0
    failed: int = 0

    @property
    def did_work(self) -> bool:
        return bool(self.applied or self.deleted or self.skipped or self.failed)


class ConnectorReconcileLoop:
    """Converges every deployed agent's connectors, forever."""

    def __init__(
        self,
        *,
        engine: AsyncEngine,
        source: ManifestSource,
        client: ConnectorClient,
        namespace: str,
        db_schema: str,
        interval_seconds: float = 60.0,
    ) -> None:
        self._engine = engine
        self._source = source
        self._client = client
        self._namespace = namespace
        self._interval = interval_seconds
        self._platform_5xx_streaks: dict[uuid.UUID, int] = {}
        # Table identifiers are not user input; the schema comes from config.
        self._sql = text(_TARGETS_SQL.format(schema=db_schema))

    async def targets(self) -> list[AgentTarget]:
        async with self._engine.connect() as conn:
            rows = (await conn.execute(self._sql)).mappings().all()
        return [
            AgentTarget(
                agent_id=row["agent_id"],
                agent_name=row["agent_name"],
                version_id=row["version_id"],
                has_bundle=bool(row["has_bundle"]),
            )
            for row in rows
        ]

    def _reconcile_one(self, target: AgentTarget) -> AgentOutcome:
        if not target.has_bundle:
            # The in-force version has no stored bundle, so the render endpoint
            # 404s for it and `raise_for_status()` would mark this agent failed
            # every pass, forever, with no pass ever able to clear it. Falling
            # back to a bundled runner-up is the #1216 bug itself. What is left
            # is the honest reading: this version declares no connectors we can
            # reach, so everything we own for the agent is undeclared and the
            # prune-only path converges it -- minus the Secret, which we must
            # not touch without a render to name it.
            return prune_agent(
                self._client,
                agent=target.agent_name,
                namespace=self._namespace,
            )
        return reconcile_agent(
            self._source,
            self._client,
            agent=target.agent_name,
            agent_id=str(target.agent_id),
            version_id=str(target.version_id),
            namespace=self._namespace,
        )

    async def one_pass(self) -> PassSummary:
        """Reconcile every deployed agent once."""

        summary = PassSummary()
        targets = await self.targets()
        for target in targets:
            summary.reconciled += 1
            try:
                outcome = await asyncio.to_thread(self._reconcile_one, target)
            except Exception as exc:
                if (
                    isinstance(exc, httpx.HTTPStatusError)
                    and 500 <= exc.response.status_code <= 599
                ):
                    streak = min(
                        self._platform_5xx_streaks.get(target.agent_id, 0) + 1,
                        _PLATFORM_5XX_GRACE_PASSES + 1,
                    )
                    self._platform_5xx_streaks[target.agent_id] = streak
                    if streak <= _PLATFORM_5XX_GRACE_PASSES:
                        logger.warning(
                            "connector reconcile platform API returned %d for agent=%s; "
                            "retrying next pass (%d/%d)",
                            exc.response.status_code,
                            target.agent_name,
                            streak,
                            _PLATFORM_5XX_GRACE_PASSES,
                        )
                        continue
                else:
                    self._platform_5xx_streaks.pop(target.agent_id, None)

                # Ends with this agent. Aborting the sweep would leave every
                # later agent unreconciled, and the order is arbitrary.
                summary.failed += 1
                logger.exception(
                    "connector reconcile raised for agent=%s; continuing with the rest",
                    target.agent_name,
                )
                continue

            self._platform_5xx_streaks.pop(target.agent_id, None)
            if outcome.skipped:
                summary.skipped += 1
                # A skip means "no operator-supplied Secret", not "nothing
                # happened": #1214 still runs a delete-only prune in that case,
                # so the report below (if any) can carry real deletes or a
                # real failure that must not vanish along with the skip.
            if outcome.report is not None:
                summary.applied += len(outcome.report.applied)
                summary.deleted += len(outcome.report.deleted)
                if not outcome.report.ok:
                    summary.failed += 1

        log = logger.info if summary.did_work else logger.debug
        log(
            "connector reconcile pass: %d agent(s), %d applied, %d deleted, %d skipped, %d failed",
            summary.reconciled,
            summary.applied,
            summary.deleted,
            summary.skipped,
            summary.failed,
        )
        self._record_pass_metrics(
            outcome="failure" if summary.failed else "success",
        )
        return summary

    def _record_pass_metrics(self, *, outcome: str) -> None:
        now = _monotonic()
        last_success = getattr(self, "_last_success_monotonic", None)
        if outcome == "success":
            self._last_success_monotonic = now
            last_success = now
        attributes = {
            "service.name": "curie-worker",
            "operation": "connector-reconciler",
            "role": "background",
            "outcome": outcome,
        }
        record_metric("curie.background.loop", attributes=attributes)
        if last_success is not None:
            record_metric(
                "curie.background.last_success.age",
                max(0.0, now - last_success),
                attributes={key: value for key, value in attributes.items() if key != "outcome"},
            )

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        """Reconcile on an interval until told to stop.

        Never raises. This shares a process with the kernel, so a reconcile
        problem must not become a worker outage -- it fails loudly, waits, and
        tries again.
        """

        stop = stop or asyncio.Event()
        logger.info(
            "connector reconcile loop started namespace=%s interval=%ss",
            self._namespace,
            self._interval,
        )
        while not stop.is_set():
            try:
                await self.one_pass()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("connector reconcile pass failed; retrying next interval")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
            except TimeoutError:
                continue
        logger.info("connector reconcile loop stopped")
