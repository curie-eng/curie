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
then the one pass that mattered scrolls by unread. A skipped agent is part of
that steady state (#1215): its WARNING is logged when it enters the skip or
its reason changes, one INFO when it leaves, and the
`curie.connector.reconcile.skipped_agents` gauge carries it between.

The cluster-shaped work is synchronous -- the Kubernetes client is, and so is
the render fetch -- so a pass runs in a worker thread rather than blocking the
event loop the kernel is using.

**Shared applications (ADR 0239) follow the agents.** Each agent's render
lists its shared entries with their member reach, or refuses with a code. The
pass records every agent's answer in a ``SharedObservation`` (authoritative,
unresolved, or gone), then ensures every declared application, applies and
prunes reach, and collects application garbage (``shared_reconcile``). A
refused render applies no dedicated change for that agent and prunes all of
its shared reach; a render the pass could not get leaves its reach alone and
blocks application garbage collection. The agents run first and read the
readiness earlier passes recorded, so a dedicated connector replaced by a
shared reference keeps its objects until the pass after its application is
first observed ready (ADR 0239 Restore 6).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

import httpx
from curie_telemetry import record_metric
from plugin_format.connector_render import (
    AmbiguousObjectName,
    direct_service_name,
    object_name,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .connector_agent import (
    AgentOutcome,
    ManifestSource,
    RenderedConnectors,
    SharedReach,
    prune_agent,
    reconcile_agent,
)
from .connector_apply import ConnectorClient
from .shared_declarations_client import WORKER_TOKEN_MISSING, shared_refusal
from .shared_reconcile import (
    AGENT_LABEL,
    COMPONENT,
    COMPONENT_LABEL,
    PART_OF_LABEL,
    SharedObservation,
    SharedOutcome,
    SharedReconciler,
)

if TYPE_CHECKING:
    from .connector_probe import ProbeTrigger

logger = logging.getLogger(__name__)
_monotonic = time.monotonic

_PLATFORM_5XX_GRACE_PASSES = 3

RENDER_MALFORMED = "shared_render_malformed"
REACH_FOREIGN = "shared_reach_foreign"
_APPLICATION_LABEL = "curietech.ai/shared-application"


class SharedRenderRefused(Exception):
    """The render refused this agent's shared declarations, or answered them malformed.

    ``declared`` is every application the bundle declares when the refusal
    names them, else None: the declarations are unknown, which blocks
    application garbage collection for the pass. ``str(exc)`` is the code.
    """

    def __init__(self, code: str, declared: tuple[str, ...] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.declared = declared

    def __str__(self) -> str:
        return self.code


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
        client: httpx.Client | None = None,
    ) -> None:
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._params = {"release": release, "namespace": namespace, "app_name": app_name}
        self._release = release
        self._namespace = namespace
        self._timeout = timeout
        # The transport seam; None builds a client per call.
        self._client = client

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        url = f"{self._base}/agents/{agent_id}/versions/{version_id}/connectors"
        if self._client is not None:
            response = self._client.get(url, params=self._params, headers=self._headers)
        else:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, params=self._params, headers=self._headers)
        refused = shared_refusal(response)
        if refused is not None:
            raise SharedRenderRefused(*refused)
        response.raise_for_status()
        body: dict[str, Any] = response.json()
        return RenderedConnectors(
            manifests=list(body.get("manifests") or []),
            owned_secret_name=str(body.get("owned_secret_name") or ""),
            owned_secret_keys=list(body.get("owned_secret_keys") or []),
            shared=self._shared(body.get("shared", []), agent_id),
        )

    def _shared(self, value: object, agent_id: str) -> tuple[SharedReach, ...]:
        """The render's shared entries, each reach object held to this agent and release.

        Anything else is the whole render refused (``shared_render_malformed``,
        or ``shared_reach_foreign`` for a reach object that is not this
        agent's), so nothing of it is applied and its declarations count as
        unknown. A render with no ``shared`` field declares none.
        """

        if not isinstance(value, list):
            raise SharedRenderRefused(RENDER_MALFORMED)
        entries: list[SharedReach] = []
        for item in value:
            if not isinstance(item, dict):
                raise SharedRenderRefused(RENDER_MALFORMED)
            name, application, reach = item.get("name"), item.get("application"), item.get("reach")
            if (
                not isinstance(name, str)
                or not isinstance(application, str)
                or not isinstance(reach, list)
                or not all(isinstance(obj, dict) for obj in reach)
            ):
                raise SharedRenderRefused(RENDER_MALFORMED)
            for obj in reach:
                self._own_reach(obj, agent_id, application)
            entries.append(SharedReach(name=name, application=application, reach=tuple(reach)))
        return tuple(entries)

    def _own_reach(self, obj: dict[str, Any], agent_id: str, application: str) -> None:
        metadata = obj.get("metadata")
        labels = metadata.get("labels") if isinstance(metadata, dict) else None
        if (
            obj.get("kind") != "NetworkPolicy"
            or not isinstance(labels, dict)
            or not isinstance(metadata, dict)
            or not isinstance(metadata.get("name"), str)
            or metadata.get("namespace", self._namespace) != self._namespace
            or labels.get(AGENT_LABEL) != agent_id
            or labels.get(COMPONENT_LABEL) != COMPONENT
            or labels.get(PART_OF_LABEL) != self._release
            or labels.get(_APPLICATION_LABEL) != application
        ):
            raise SharedRenderRefused(REACH_FOREIGN)


class _SharedGuard:
    """The render source, refusing shared entries the loop cannot reconcile.

    Without a shared reconciler that can reach the worker routes (no internal
    worker token), a render that carries shared entries is refused before any
    of it is applied, as undecidable: the agent's shared reach is pruned and
    application garbage collection waits, while dedicated-only agents are
    unaffected.

    Otherwise each shared entry whose application the reconciler has not
    observed ready marks the dedicated objects of the same alias retained
    (``RenderedConnectors.retained``), by the frozen render's names. An alias
    those names refuse (``AmbiguousObjectName``) retains nothing.
    """

    def __init__(
        self, inner: ManifestSource, *, shared: SharedReconciler | None, agent: str
    ) -> None:
        self._inner = inner
        self._shared = shared
        self._agent = agent

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        rendered = self._inner.rendered(agent_id=agent_id, version_id=version_id)
        if not rendered.shared:
            return rendered
        shared = self._shared
        if shared is None or not shared.ready:
            raise SharedRenderRefused(WORKER_TOKEN_MISSING)
        retained: set[str] = set()
        for entry in rendered.shared:
            if shared.application_ready(entry.application):
                continue
            try:
                retained.update(
                    (
                        object_name(shared.release, self._agent, entry.name),
                        direct_service_name(shared.release, self._agent, entry.name),
                    )
                )
            except AmbiguousObjectName:
                # An alias the frozen render refuses to name can never have
                # been a dedicated connector, so there is nothing to retain.
                continue
        return replace(rendered, retained=frozenset(retained))


class _ObservingClient:
    """The reconcile's client, keeping the Deployments ``list_owned`` returned.

    @spec ACTION-EXECUTOR-13. Every verb passes straight through, so the
    reconcile's decisions and writes are the unwrapped client's. Only
    Deployments are kept: the owned Secret is listed too and has no business
    outliving the call.
    """

    def __init__(self, inner: ConnectorClient) -> None:
        self._inner = inner
        self.deployments: list[dict[str, Any]] = []

    def list_owned(self, namespace: str, owner: str) -> list[dict[str, Any]]:
        live = self._inner.list_owned(namespace, owner)
        self.deployments = [obj for obj in live if obj.get("kind") == "Deployment"]
        return live

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        self._inner.apply(namespace, obj)

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self._inner.delete(namespace, kind, name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


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
    # Agents whose render this pass could not get: their shared reach is
    # kept, and shared application garbage collection waits.
    shared_unresolved: int = 0

    @property
    def did_work(self) -> bool:
        # `skipped` is deliberately absent (#1215): a skip persists every pass
        # until someone acts, and its transitions carry their own log lines.
        return bool(self.applied or self.deleted or self.failed)


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
        probe_trigger: ProbeTrigger | None = None,
        shared: SharedReconciler | None = None,
    ) -> None:
        self._engine = engine
        # ADR 0239. None, or one without the internal worker token, cannot
        # reconcile a shared application: an agent whose render carries shared
        # entries is then refused as undecidable.
        self._shared = shared
        # ACTION-EXECUTOR-13: asks for a capability probe from what a pass
        # observed. None is the loop without it.
        self._probe_trigger = probe_trigger
        self._source = source
        self._client = client
        self._namespace = namespace
        self._interval = interval_seconds
        self._platform_5xx_streaks: dict[uuid.UUID, int] = {}
        # The reason each currently-skipped agent was last logged with. Kept
        # in process only: a restarted worker logs each standing skip once.
        self._skip_reasons: dict[uuid.UUID, str] = {}
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
        return self._reconcile_with(target, self._client)

    def _reconcile_with(self, target: AgentTarget, client: ConnectorClient) -> AgentOutcome:
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
                client,
                agent=target.agent_name,
                namespace=self._namespace,
            )
        return reconcile_agent(
            _SharedGuard(self._source, shared=self._shared, agent=target.agent_name),
            client,
            agent=target.agent_name,
            agent_id=str(target.agent_id),
            version_id=str(target.version_id),
            namespace=self._namespace,
        )

    async def one_pass(self) -> PassSummary:
        """Reconcile every deployed agent once."""

        summary = PassSummary()
        targets = await self.targets()
        deployed = {target.agent_id for target in targets}
        observation = SharedObservation(deployed={str(agent_id) for agent_id in deployed})
        for agent_id in [known for known in self._skip_reasons if known not in deployed]:
            # No longer deployed, so no longer skipped. Dropped without a line:
            # the undeploy is the event, and this loop did not cause it.
            del self._skip_reasons[agent_id]
        for target in targets:
            summary.reconciled += 1
            observing = _ObservingClient(self._client) if self._probe_trigger is not None else None
            try:
                if observing is None:
                    outcome = await asyncio.to_thread(self._reconcile_one, target)
                else:
                    outcome = await asyncio.to_thread(self._reconcile_with, target, observing)
            except SharedRenderRefused as refused:
                # An answer, not an outage: no dedicated change for this agent
                # this pass, and all of its shared reach is pruned.
                self._platform_5xx_streaks.pop(target.agent_id, None)
                declared = refused.declared
                if declared and not (self._shared is not None and self._shared.ready):
                    refused, declared = SharedRenderRefused(WORKER_TOKEN_MISSING), None
                observation.refused(str(target.agent_id), declared)
                summary.failed += 1
                logger.warning(
                    "connector reconcile render refused for agent=%s code=%s",
                    target.agent_name,
                    refused.code,
                )
                continue
            except Exception as exc:
                observation.unresolvable(str(target.agent_id))
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
            rendered = outcome.rendered
            observation.rendered(str(target.agent_id), rendered.shared if rendered else ())
            if observing is not None:
                await self._request_probes(target, outcome, observing)
            self._note_skip(target, outcome.skipped)
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

        summary.shared_unresolved = len(observation.unresolved)
        if self._shared is not None:
            await self._reconcile_shared(self._shared, observation, summary)

        log = logger.info if summary.did_work else logger.debug
        log(
            "connector reconcile pass: %d agent(s), %d applied, %d deleted, %d skipped, "
            "%d failed, %d shared unresolved",
            summary.reconciled,
            summary.applied,
            summary.deleted,
            summary.skipped,
            summary.failed,
            summary.shared_unresolved,
        )
        self._record_pass_metrics(
            outcome="failure" if summary.failed else "success",
        )
        return summary

    async def _reconcile_shared(
        self, shared: SharedReconciler, observation: SharedObservation, summary: PassSummary
    ) -> None:
        """Applications, then reach, then garbage; each failure ends with its own step.

        Every declared application is ensured, so a disabled application, or
        one whose members' renders refuse while still declaring it, keeps its
        objects. Reach runs only after, and garbage last, from the same
        observation.
        """

        def fold(outcome: SharedOutcome) -> None:
            summary.applied += outcome.applied
            summary.deleted += outcome.deleted
            if outcome.failures:
                summary.failed += 1

        if shared.ready:
            for application_id in sorted(observation.declared_applications()):
                try:
                    fold(await asyncio.to_thread(shared.ensure_application, application_id))
                except Exception:
                    summary.failed += 1
                    logger.exception(
                        "shared application reconcile raised for application=%s; "
                        "continuing with the rest",
                        application_id,
                    )
        for step, name in (
            (shared.reconcile_reach, "reach"),
            (shared.collect_garbage, "garbage collection"),
        ):
            try:
                outcome = await asyncio.to_thread(step, observation)
            except Exception:
                summary.failed += 1
                logger.exception("shared connector %s raised; retrying next pass", name)
                continue
            for failure in outcome.failures:
                logger.warning("shared connector %s failed: %s", name, failure)
            fold(outcome)

    async def _request_probes(
        self, target: AgentTarget, outcome: AgentOutcome, observing: _ObservingClient
    ) -> None:
        """Hand the pass's observation to the probe trigger. Never raises.

        Only after a reconcile that completed for this agent, and never for a
        Deployment the same pass planned to delete: that one is going away, so
        no probe can be attributed to it.
        """

        trigger = self._probe_trigger
        if trigger is None or not observing.deployments:
            return
        try:
            await trigger.after_reconcile(
                agent_id=str(target.agent_id),
                agent_name=target.agent_name,
                observed=observing.deployments,
                deleting=outcome.plan.delete if outcome.plan is not None else (),
            )
        except Exception:
            logger.exception("capability probe trigger raised for agent=%s", target.agent_name)

    def _note_skip(self, target: AgentTarget, reason: str | None) -> None:
        """Log a skip only when it starts, changes or ends.

        An agent whose reconcile raised is not seen here, so its skip state
        stands until a pass completes for it.
        """

        previous = self._skip_reasons.get(target.agent_id)
        if reason is not None:
            if reason != previous:
                self._skip_reasons[target.agent_id] = reason
                logger.warning(
                    "connector reconcile skipped agent=%s: %s", target.agent_name, reason
                )
            return
        if previous is not None:
            del self._skip_reasons[target.agent_id]
            logger.info("connector reconcile agent=%s is no longer skipped", target.agent_name)

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
        record_metric(
            "curie.connector.reconcile.skipped_agents",
            len(self._skip_reasons),
            attributes={"service.name": "curie-worker"},
        )
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
