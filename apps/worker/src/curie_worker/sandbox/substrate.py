"""The sandbox lifecycle substrate: claim, route, suspend/resume, reap.

The one-live-session-per-thread routing rule (detailed-architecture section 2)
is implemented here as: the affinity store maps ``thread_key`` to exactly one
claim; ``claim()`` returns the existing live binding or creates one from the
warm pool; a lost creation race is resolved by deleting the loser's claim and
adopting the winner's. F1 (the worker kernel) composes these primitives; it
never touches Kubernetes directly.

Cold-restart rehydrate design (ADR-0003, PT-1 finding: suspend/resume is a cold
pod restart, the live process never survives): ``suspend()`` records the
caller-supplied history ref on the route; ``resume()`` retires the suspended
claim and creates a NEW claim whose per-claim env injects
``CURIE_HISTORY_REF`` (and the original ``CURIE_SESSION_ID``), so the
replacement runner boots rehydrating from stored history rather than assuming
process or cache warmth. The runner resolves the ref to the thread's transcript
namespace on the durable state store and replays the prior turns as a boot-time
system-prompt preamble; the ref is a harness-agnostic state-store URL, not an SDK
resume id (ADR-0029 superseded that framing). Producing the ref -- the thread's
transcript key -- is the caller's job.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

from aci_protocol import BootEnv
from curie_telemetry import operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode

from ..binding import MAX_TURNS_ENV, RUNNER_TOKEN_ENV
from ..workitem_dispatch import TerminationObservation
from .affinity import AffinityStore
from .types import (
    AGENT_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    THREAD_HASH_LABEL,
    CapacityExhaustedError,
    ClaimTimeoutError,
    NoRouteError,
    PressureScanResult,
    QuotaRejection,
    RouteChangedError,
    RouteRecord,
    RouteState,
    SandboxClient,
    SandboxHandle,
    SandboxView,
    SubstrateConfig,
    SuspendedThreadError,
    claim_warm_pool,
)

# The resume overlay writes these into the replacement claim's per-claim env, and
# the runner reads them out of the same boot contract -- same consumer, so both
# are named from the ONE declaration in ``aci_protocol.BootEnv`` (#488, ADR-0049)
# rather than retyped. A local literal would drift silently on a rename: the
# replacement sandbox boots and answers, having quietly lost its history.
HISTORY_ENV = BootEnv.env_key("history_ref")
SESSION_ENV = BootEnv.env_key("session_id")

logger = logging.getLogger(__name__)

# Leeway added to ``claim_timeout_seconds`` before the reaper will treat an
# unrouted claim as litter. Deliberately generous, for four reasons that all
# push the measured age HIGHER than the true age, which is the direction that
# reaps a live claim early:
#
# 1. Kubernetes serializes ``creationTimestamp`` at whole-second granularity,
#    so a claim can read up to 1s older than it is.
# 2. Nothing keeps the worker's clock and the API server's clock in sync;
#    single-digit seconds of skew is routine, more on a loaded CI node, which
#    is exactly where this race fires.
# 3. ``claim_timeout_seconds`` does not start until _claim_fresh arms its
#    deadline, and ``create_claim`` returns before that with NO request
#    timeout on the underlying API call, so the claim already exists (and
#    already looks reapable) for the whole create-call latency first.
# 4. The put_if_absent race-resolution tail runs affinity reads and
#    ``get_sandbox`` calls, also with no request timeout, AFTER the deadline
#    has passed and therefore entirely outside the budget.
#
# The margin is what absorbs 3 and 4, which nothing else bounds today. Adding
# request timeouts to the Kubernetes calls is a separate change, not this one.
#
# The asymmetry is why the number is padded rather than tuned, and it is the
# reason not to "optimize" it toward zero: overshooting costs at most 30 extra
# seconds of claim retention against a 3600s route TTL (under 1% of worst-case
# litter lifetime, zero steady-state cost), while undershooting kills a live
# sandbox under a blocked turn. A stall that exceeds even this margin does not
# lose data: the claim is reaped, the creator's route then points at a gone
# sandbox, and the next claim() takes the existing _evict_stale path, which
# drops the stale route and rebinds. Slow turn, not corruption.
REAP_GRACE_MARGIN_SECONDS = 30.0
_CONTROL_REQUEST_TIMEOUT_S = 5.0
_GONE_READ_TIMEOUT_S = 1.0


def _poll_sleeps(config: SubstrateConfig) -> Iterator[float]:
    """Yield the successive sleep lengths for ONE substrate wait loop.

    The first ``poll_fast_polls`` sleeps are the configured interval, so the
    warm-pool fast path polls exactly as often as the old fixed loop did; after
    that each sleep grows by ``poll_backoff_factor`` up to the cap. The result
    is that a cold boot costs tens of ``get_claim``/``get_sandbox`` calls rather
    than hundreds, without making a warm bind any slower to notice.

    Each wait loop takes its own generator, so the serviceFQDN phase restarts at
    the fast interval: it begins the moment the claim binds, which is exactly
    when the sandbox address is about to appear, and inheriting the bind phase's
    backed-off interval would add half a second to every cold claim.

    The generator is deliberately clock-free. Bounding a sleep by the shared
    deadline is the caller's job, because only the caller knows the deadline.
    """

    interval = config.poll_interval_seconds
    cap = max(config.poll_interval_max_seconds, config.poll_interval_seconds)
    for _ in range(max(0, config.poll_fast_polls)):
        yield interval
    while True:
        interval = min(interval * config.poll_backoff_factor, cap)
        yield interval


def _sandbox_attributes(operation: str, outcome: str) -> dict[str, str]:
    return {
        "service.name": "curie-worker",
        "operation": operation,
        "outcome": outcome,
    }


def _record_inventory(*, active: float, suspended: float) -> None:
    # Both inventories intentionally use one fixed series each. Lifecycle
    # operation labels belong on curie.sandbox.lifecycle; putting claim/release
    # on a state gauge creates several stale last-value series whose sum cannot
    # represent the current inventory.
    attributes = _sandbox_attributes("observe", "observed")
    record_metric("curie.sandbox.active", active, attributes=attributes)
    record_metric("curie.sandbox.suspended", suspended, attributes=attributes)


class SandboxSubstrate:
    """Provision, route, and reap runner sandboxes for conversation threads."""

    def __init__(
        self,
        k8s: SandboxClient,
        affinity: AffinityStore,
        config: SubstrateConfig,
    ) -> None:
        self._k8s = k8s
        self._affinity = affinity
        self._config = config
    # -- claim / lookup -------------------------------------------------------

    def claim(
        self,
        thread_key: str,
        *,
        env: dict[str, str] | None = None,
        agent_name: str | None = None,
        workspace_repo: str | None = None,
        workspace_materialized_head: str | None = None,
        publication_visible_outcome_revision: int = 0,
        fresh_only: bool = False,
    ) -> SandboxHandle:
        """Return the thread's live sandbox, claiming a warm one if needed.

        ``env`` is per-claim env injection (the resume path uses it for the
        history ref); the fast path passes none so the claim binds a pre-warmed
        generic sandbox. ``agent_name`` selects the per-agent warm pool when
        connector secrets are marked on ``env`` (#1488).

        ``fresh_only`` is for callers whose ``env`` must reach the runner that
        serves the turn (attachment staging, #2739): a live running route, or
        a concurrent winner of the route race, raises ``RouteChangedError``
        instead of being reused. Suspended routes still raise
        ``SuspendedThreadError``.
        """

        started = time.monotonic()
        handle: SandboxHandle | None = None
        error: Exception | None = None
        outcome = "failed"
        with operation_span(
            "curie.sandbox.claim",
            kind=SpanKind.INTERNAL,
            attributes={"service.name": "curie-worker", "operation": "claim"},
        ) as span:
            try:
                record = self._affinity.get(thread_key)
                if record is not None:
                    if record.state is RouteState.SUSPENDED:
                        raise SuspendedThreadError(thread_key)
                    sandbox = self._k8s.get_sandbox(
                        record.handle.sandbox_name,
                        request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                    )
                    if sandbox is not None and sandbox.operating_mode == "Running":
                        if fresh_only:
                            raise RouteChangedError(thread_key)
                        self._affinity.touch(thread_key, self._config.route_ttl_seconds)
                        handle = record.handle
                        outcome = "reused"
                    else:
                        # Never hand back a stale route or let it win the fresh claim.
                        self._evict_stale(thread_key, record)
                if handle is None:
                    handle = self._claim_fresh(
                        thread_key,
                        env=env,
                        state=RouteState.LIVE,
                        agent_name=agent_name,
                        workspace_repo=workspace_repo,
                        workspace_materialized_head=workspace_materialized_head,
                        publication_visible_outcome_revision=(
                            publication_visible_outcome_revision
                        ),
                        fresh_only=fresh_only,
                    )
                    outcome = "claimed"
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "sandbox.claim.failed",
                    {"outcome": "failed", "error.class": type(exc).__name__},
                )
            else:
                span.add_event("sandbox.claim.completed", {"outcome": outcome})

        attributes = _sandbox_attributes("claim", outcome)
        record_metric("curie.sandbox.lifecycle", attributes=attributes)
        record_metric(
            "curie.sandbox.claim.duration",
            max(0.0, time.monotonic() - started),
            attributes=attributes,
        )
        if error is not None:
            raise error
        assert handle is not None
        return handle

    def lookup(self, thread_key: str) -> SandboxHandle | None:
        """The thread's live handle, or None (no route, suspended, or the
        cluster-side sandbox is gone/not ready)."""

        record = self._affinity.get(thread_key)
        if record is None or record.state is not RouteState.LIVE:
            return None
        sandbox = self._k8s.get_sandbox(
            record.handle.sandbox_name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
        )
        if sandbox is None or sandbox.operating_mode != "Running":
            return None
        return record.handle

    @property
    def claim_timeout_seconds(self) -> float:
        """The fresh claim budget the pressure path must reserve for its retry."""

        return self._config.claim_timeout_seconds

    @property
    def namespace(self) -> str:
        """The only namespace whose routes this substrate may reclaim."""

        return self._config.namespace

    async def pressure_candidates(
        self, *, max_pages: int, max_records: int, deadline: float
    ) -> PressureScanResult:
        """Return a complete, namespace local pressure inventory."""

        result = await self._affinity.pressure_candidates(
            max_pages=max_pages,
            max_records=max_records,
            deadline=deadline,
        )
        if result.outcome != "complete":
            return result
        return PressureScanResult(
            tuple(
                candidate
                for candidate in result.candidates
                if candidate.record.handle.namespace == self._config.namespace
            ),
            "complete",
        )

    async def pressure_get(self, thread_key: str) -> RouteRecord | None:
        """Reread one candidate on the bounded pressure connection."""

        return await self._affinity.pressure_get(thread_key)

    async def detach_if_unchanged(
        self,
        thread_key: str,
        *,
        expected_claim: str,
        expected_generation: int,
        expected_expires_at_ms: int,
        lock_key: str,
        lock_token: str,
    ) -> bool:
        """Detach one exact route while its distributed victim lock is owned."""

        return await self._affinity.detach_if_unchanged(
            thread_key,
            expected_claim=expected_claim,
            expected_generation=expected_generation,
            expected_expires_at_ms=expected_expires_at_ms,
            lock_key=lock_key,
            lock_token=lock_token,
        )

    def workspace_repository(self, thread_key: str) -> str | None:
        """The persisted workspace repository for any route state on a thread."""

        record = self._affinity.get(thread_key)
        return None if record is None else record.handle.workspace_repo

    def adopt(self, thread_key: str) -> SandboxHandle | None:
        """Adopt an existing ready live route without ever creating one.

        This is the workspace-safe route primitive: callers can distinguish a
        reusable sandbox from every state that needs a freshly prepared
        workspace reference without a ``lookup()`` then ``claim()`` gap.  A
        suspended route is preserved for ``resume()``; a live route whose
        claim or sandbox is stale or unready is evicted so a subsequent fresh
        claim cannot accidentally inherit it.
        """

        record = self._affinity.get(thread_key)
        if record is None or record.state is not RouteState.LIVE:
            return None

        claim = self._k8s.get_claim(
            record.handle.claim_name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
        )
        if (
            claim is None
            or not claim.ready
            or claim.sandbox_name != record.handle.sandbox_name
        ):
            self._evict_stale(thread_key, record)
            return None

        sandbox = self._k8s.get_sandbox(
            record.handle.sandbox_name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
        )
        if (
            sandbox is None
            or not sandbox.ready
            or sandbox.operating_mode != "Running"
        ):
            self._evict_stale(thread_key, record)
            return None

        if not self._affinity.touch(thread_key, self._config.route_ttl_seconds):
            return None
        return record.handle

    def handoff(
        self,
        thread_key: str,
        *,
        expected: SandboxHandle,
        env: dict[str, str],
        workspace_repo: str | None,
        workspace_materialized_head: str | None = None,
        publication_visible_outcome_revision: int = 0,
        agent_name: str | None = None,
        validate_candidate: Callable[[SandboxHandle], None] | None = None,
    ) -> SandboxHandle:
        """Cold create a runner, then CAS it over one retained route.

        The old route remains authoritative while the candidate binds. Losing
        the claim+generation fence deletes only the unexposed candidate. After
        a successful swap the old claim is cleanup-only; a failed deletion is
        intentionally recoverable by the ordinary orphan reaper.
        """

        boot = dict(env)
        boot[SESSION_ENV] = expected.session_id
        if expected.history_ref is not None:
            boot[HISTORY_ENV] = expected.history_ref
        candidate = self._claim_fresh(
            thread_key,
            env=boot,
            state=RouteState.LIVE,
            session_id=expected.session_id,
            history_ref=expected.history_ref,
            agent_name=agent_name,
            workspace_repo=workspace_repo,
            workspace_materialized_head=workspace_materialized_head,
            publication_visible_outcome_revision=publication_visible_outcome_revision,
            generation=expected.generation + 1,
            publish=False,
        )
        try:
            if validate_candidate is not None:
                validate_candidate(candidate)
        except Exception:
            # The candidate is ready but still unrouted. Refusal retires only
            # that unexposed claim; the old route remains authoritative until
            # the generation CAS below succeeds.
            self._k8s.delete_claim(
                candidate.claim_name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            )
            raise
        record = RouteRecord(handle=candidate, state=RouteState.LIVE)
        if not self._affinity.replace_if_generation(
            thread_key,
            expected_claim=expected.claim_name,
            expected_generation=expected.generation,
            record=record,
            ttl_seconds=self._config.route_ttl_seconds,
        ):
            self._k8s.delete_claim(
                candidate.claim_name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            )
            raise NoRouteError(f"late handoff lost its route fence for {thread_key}")
        try:
            self._k8s.delete_claim(
                expected.claim_name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            )
        except Exception:  # noqa: BLE001 - route already swapped; reaper owns cleanup
            logger.exception("late handoff left old claim for orphan reaping")
        return candidate

    # -- suspend / resume -------------------------------------------------------

    def suspend(self, thread_key: str, *, history_ref: str | None) -> None:
        """Suspend the thread's sandbox and record the rehydrate ref.

        The pod is deleted by the controller (PT-1: suspend is pod deletion);
        the route flips to SUSPENDED with the longer TTL so ``resume()`` can
        rebuild session state later.
        """

        error: Exception | None = None
        record: RouteRecord | None = None
        with operation_span(
            "curie.sandbox.suspend",
            kind=SpanKind.INTERNAL,
            attributes={"service.name": "curie-worker", "operation": "suspend"},
        ) as span:
            try:
                record = self._affinity.get(thread_key)
                if record is None:
                    raise NoRouteError(thread_key)
                self._k8s.set_sandbox_mode(record.handle.sandbox_name, "Suspended")
                self._affinity.mark_suspended(
                    thread_key, history_ref, self._config.suspended_route_ttl_seconds
                )
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "sandbox.suspend.failed",
                    {"outcome": "failed", "error.class": type(exc).__name__},
                )
            else:
                span.add_event("sandbox.suspended", {"outcome": "suspended"})
        outcome = "failed" if error is not None else "suspended"
        record_metric(
            "curie.sandbox.lifecycle",
            attributes=_sandbox_attributes("suspend", outcome),
        )
        if error is not None:
            raise error
        assert record is not None

    def resume(
        self,
        thread_key: str,
        *,
        env: dict[str, str] | None = None,
        agent_name: str | None = None,
        workspace_repo: str | None = None,
        workspace_materialized_head: str | None = None,
        publication_visible_outcome_revision: int | None = None,
    ) -> SandboxHandle:
        """Rehydrate a suspended thread into a fresh claim.

        The suspended claim is retired (its process and cache are gone either
        way) and a new claim is created with ``CURIE_HISTORY_REF`` injected,
        so the replacement runner boots resuming from stored history.

        ``env`` is the caller's bound boot env (bundle ref, budget, state
        refs), the same one a fresh ``claim()`` would inject. The suspended
        pod was deleted (ADR-0003), so the replacement boots from env alone;
        resuming without it would boot a generic, bundle-less runner. The
        session identity and any recorded history ref are preserved on top,
        and the runner token is minted fresh when the caller did not already
        mint one (issue #63: the old token died with the old claim).
        """

        started = time.monotonic()
        handle: SandboxHandle | None = None
        old: SandboxHandle | None = None
        error: Exception | None = None
        with operation_span(
            "curie.sandbox.resume",
            kind=SpanKind.INTERNAL,
            attributes={"service.name": "curie-worker", "operation": "resume"},
        ) as span:
            try:
                record = self._affinity.get(thread_key)
                if record is None:
                    raise NoRouteError(thread_key)
                old = record.handle
                boot = dict(env) if env else {}
                boot.setdefault(SESSION_ENV, old.session_id)
                if not boot.get(RUNNER_TOKEN_ENV):
                    boot[RUNNER_TOKEN_ENV] = secrets.token_urlsafe(32)
                if old.history_ref is not None:
                    boot.setdefault(HISTORY_ENV, old.history_ref)

                self._k8s.delete_claim(
                    old.claim_name,
                    request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                )
                self._affinity.delete_if_claim(thread_key, old.claim_name)
                handle = self._claim_fresh(
                    thread_key,
                    env=boot,
                    state=RouteState.LIVE,
                    session_id=old.session_id,
                    history_ref=old.history_ref,
                    agent_name=agent_name,
                    workspace_repo=workspace_repo or old.workspace_repo,
                    workspace_materialized_head=(
                        workspace_materialized_head or old.workspace_materialized_head
                    ),
                    publication_visible_outcome_revision=(
                        publication_visible_outcome_revision
                        if publication_visible_outcome_revision is not None
                        else old.publication_visible_outcome_revision
                    ),
                    generation=old.generation + 1,
                )
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "sandbox.resume.failed",
                    {"outcome": "failed", "error.class": type(exc).__name__},
                )
            else:
                span.add_event("sandbox.resumed", {"outcome": "resumed"})
        outcome = "failed" if error is not None else "resumed"
        attributes = _sandbox_attributes("resume", outcome)
        record_metric("curie.sandbox.lifecycle", attributes=attributes)
        record_metric(
            "curie.sandbox.resume.duration",
            max(0.0, time.monotonic() - started),
            attributes=attributes,
        )
        if error is not None:
            raise error
        assert handle is not None and old is not None
        return handle

    # -- release / reap -------------------------------------------------------

    def release(self, thread_key: str, *, wait_gone: bool = False) -> bool:
        """End the thread's session: delete the claim (the claim's lifecycle
        deletes its sandbox and pod) and drop the route. True if a route
        existed.

        Kubernetes delete of a SandboxClaim returns while the object (and its
        pod) still exist under a deletionTimestamp. ``wait_gone=True`` polls
        until ``get_claim`` is None so ResourceQuota is actually free before
        the caller continues (#2259). A wait that times out is logged, not
        raised: the delete was issued, and a following claim is no worse off
        than today's fire-and-forget path. Operator reset-thread keeps the
        default False so it stays inside the kernel's 5s release cap.
        """

        started = time.monotonic()
        released = False
        record: RouteRecord | None = None
        error: Exception | None = None
        with operation_span(
            "curie.sandbox.release",
            kind=SpanKind.INTERNAL,
            attributes={"service.name": "curie-worker", "operation": "release"},
        ) as span:
            try:
                record = self._affinity.get(thread_key)
                if record is not None:
                    claim_name = record.handle.claim_name
                    sandbox_name = record.handle.sandbox_name
                    self._k8s.delete_claim(
                        claim_name,
                        request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                    )
                    self._affinity.delete_if_claim(thread_key, claim_name)
                    released = True
                    if wait_gone:
                        self._await_quota_freed(claim_name, sandbox_name)
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "sandbox.release.failed",
                    {"outcome": "failed", "error.class": type(exc).__name__},
                )
            else:
                span.add_event(
                    "sandbox.released",
                    {"outcome": "released" if released else "observed"},
                )
        outcome = "failed" if error is not None else ("released" if released else "observed")
        attributes = _sandbox_attributes("release", outcome)
        record_metric("curie.sandbox.lifecycle", attributes=attributes)
        record_metric(
            "curie.sandbox.release.duration",
            max(0.0, time.monotonic() - started),
            attributes=attributes,
        )
        if error is not None:
            raise error
        return released

    def terminate_thread(
        self,
        thread_key: str,
        *,
        claim_name: str | None,
        sandbox_name: str | None,
        observer: str = "",
    ) -> TerminationObservation | None:
        """Delete every claim for the thread and observe claim+sandbox absence.

        SQL-stored names from ``start`` are always in the target set, plus any
        ``list_claims`` match on the thread hash and the affinity record. An
        empty listing is not success while a named sandbox is still present.
        Timeout returns None and keeps the stored names for a later retry.
        """

        observation: TerminationObservation | None = None
        error: Exception | None = None
        timed_out = False
        with operation_span(
            "curie.sandbox.terminate",
            kind=SpanKind.INTERNAL,
            attributes={"service.name": "curie-worker", "operation": "terminate"},
        ) as span:
            try:
                observation = self._observe_thread_gone(
                    thread_key,
                    claim_name=claim_name,
                    sandbox_name=sandbox_name,
                    observer=observer,
                )
                if observation is None:
                    timed_out = True
                    span.add_event(
                        "sandbox.terminate.timeout",
                        {"outcome": "timeout"},
                    )
                else:
                    span.add_event(
                        "sandbox.terminated",
                        {"outcome": "terminated"},
                    )
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "sandbox.terminate.failed",
                    {"outcome": "failed", "error.class": type(exc).__name__},
                )
        if error is not None:
            outcome = "failed"
        elif timed_out:
            outcome = "timeout"
        else:
            outcome = "terminated"
        attributes = _sandbox_attributes("terminate", outcome)
        record_metric("curie.sandbox.lifecycle", attributes=attributes)
        if error is not None:
            raise error
        return observation

    def _observe_thread_gone(
        self,
        thread_key: str,
        *,
        claim_name: str | None,
        sandbox_name: str | None,
        observer: str,
    ) -> TerminationObservation | None:
        claim_names: set[str] = set()
        sandbox_names: set[str] = set()
        if claim_name:
            claim_names.add(claim_name)
        if sandbox_name:
            sandbox_names.add(sandbox_name)
        record = self._affinity.get(thread_key)
        if not claim_names and not sandbox_names and record is not None:
            claim_names.add(record.handle.claim_name)
            sandbox_names.add(record.handle.sandbox_name)
        if not claim_names:
            thread_hash = hashlib.sha256(thread_key.encode("utf-8")).hexdigest()[:10]
            try:
                labelled = self._k8s.list_claims(
                    label_selector=f"{THREAD_HASH_LABEL}={thread_hash}"
                )
            except Exception as exc:  # noqa: BLE001 - still terminate known names
                logger.warning(
                    "terminate could not list claims for thread %s: %s",
                    thread_key,
                    type(exc).__name__,
                )
                labelled = []
            for view in labelled:
                claim_names.add(view.name)
                if view.sandbox_name:
                    sandbox_names.add(view.sandbox_name)
        affinity_claim = record.handle.claim_name if record is not None else None
        for name in list(claim_names):
            try:
                self._k8s.delete_claim(
                    name,
                    request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                )
            except Exception as exc:  # noqa: BLE001 - absence poll still decides
                logger.warning(
                    "terminate delete of claim %s failed: %s",
                    name,
                    type(exc).__name__,
                )
            if affinity_claim is not None and name == affinity_claim:
                self._affinity.delete_if_claim(thread_key, name)

        deadline = time.monotonic() + self._config.release_gone_timeout_seconds
        sleeps = _poll_sleeps(self._config)
        while time.monotonic() < deadline:
            remaining_claims = set()
            remaining_sandboxes = set()
            timeout = min(
                _GONE_READ_TIMEOUT_S,
                max(0.001, deadline - time.monotonic()),
            )
            for name in claim_names:
                try:
                    claim_view = self._k8s.get_claim(
                        name,
                        request_timeout_seconds=timeout,
                    )
                except Exception as exc:  # noqa: BLE001 - absence is not proven
                    logger.warning(
                        "terminate gone wait could not read claim %s: %s",
                        name,
                        type(exc).__name__,
                    )
                    remaining_claims.add(name)
                    continue
                if claim_view is not None:
                    remaining_claims.add(name)
                    if claim_view.sandbox_name:
                        sandbox_names.add(claim_view.sandbox_name)
            timeout = min(
                _GONE_READ_TIMEOUT_S,
                max(0.001, deadline - time.monotonic()),
            )
            for name in sandbox_names:
                try:
                    sandbox_view = self._k8s.get_sandbox(
                        name,
                        request_timeout_seconds=timeout,
                    )
                except Exception as exc:  # noqa: BLE001 - absence is not proven
                    logger.warning(
                        "terminate gone wait could not read sandbox %s: %s",
                        name,
                        type(exc).__name__,
                    )
                    remaining_sandboxes.add(name)
                    continue
                if sandbox_view is not None:
                    remaining_sandboxes.add(name)
            if not remaining_claims and not remaining_sandboxes:
                return TerminationObservation(
                    claims=tuple(sorted(claim_names)),
                    sandboxes=tuple(sorted(sandbox_names)),
                    observed_at=datetime.now(UTC),
                    observer=observer,
                )
            time.sleep(max(0.0, min(next(sleeps), deadline - time.monotonic())))
        logger.warning(
            "terminate of thread %s timed out after %.1fs; claims=%s sandboxes=%s",
            thread_key,
            self._config.release_gone_timeout_seconds,
            ",".join(sorted(claim_names)) or "-",
            ",".join(sorted(sandbox_names)) or "-",
        )
        return None

    def _await_quota_freed(self, claim_name: str, sandbox_name: str) -> None:
        """Poll until the deleted claim AND its sandbox are absent.

        ResourceQuota charges the pod, not the SandboxClaim. A default
        background delete can hide the CR while the pod is still terminating,
        which is the #2259 race: eval reports, the CLI starts cluster message,
        and the new claim waits on quota the dying pod still holds. Timeout
        logs and returns: the delete was issued, and a stuck finalizer must
        not turn a successful eval report into a CLI hang.
        """

        deadline = time.monotonic() + self._config.release_gone_timeout_seconds
        sleeps = _poll_sleeps(self._config)
        while time.monotonic() < deadline:
            claim_gone = False
            try:
                claim_gone = self._k8s.get_claim(
                    claim_name,
                    request_timeout_seconds=min(
                        _GONE_READ_TIMEOUT_S,
                        max(0.001, deadline - time.monotonic()),
                    ),
                ) is None
            except Exception as exc:  # noqa: BLE001 - the gone wait is soft
                logger.warning(
                    "release gone wait could not read claim: %s",
                    type(exc).__name__,
                )

            sandbox_gone = False
            try:
                sandbox_gone = self._k8s.get_sandbox(
                    sandbox_name,
                    request_timeout_seconds=min(
                        _GONE_READ_TIMEOUT_S,
                        max(0.001, deadline - time.monotonic()),
                    ),
                ) is None
            except Exception as exc:  # noqa: BLE001 - the gone wait is soft
                logger.warning(
                    "release gone wait could not read sandbox: %s",
                    type(exc).__name__,
                )
            if claim_gone and sandbox_gone:
                return
            time.sleep(max(0.0, min(next(sleeps), deadline - time.monotonic())))
        logger.warning(
            "sandbox claim %s / sandbox %s still present %.1fs after delete; "
            "quota may stay held until the controller finishes (#2259)",
            claim_name,
            sandbox_name,
            self._config.release_gone_timeout_seconds,
        )

    def delete_detached(
        self,
        record: RouteRecord,
        rejection: QuotaRejection,
        *,
        deadline: float,
    ) -> bool:
        """Delete one detached claim and prove exact quota headroom."""

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            self._k8s.delete_claim(
                record.handle.claim_name,
                request_timeout_seconds=min(
                    _CONTROL_REQUEST_TIMEOUT_S, remaining
                ),
            )
            sleeps = _poll_sleeps(self._config)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                claim_gone = self._k8s.get_claim(
                    record.handle.claim_name,
                    request_timeout_seconds=min(_GONE_READ_TIMEOUT_S, remaining),
                ) is None

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                sandbox_gone = self._k8s.get_sandbox(
                    record.handle.sandbox_name,
                    request_timeout_seconds=min(_GONE_READ_TIMEOUT_S, remaining),
                ) is None
                if claim_gone and sandbox_gone:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    try:
                        if self._k8s.quota_has_headroom(
                            rejection,
                            request_timeout_seconds=min(
                                _GONE_READ_TIMEOUT_S, remaining
                            ),
                        ):
                            return True
                    except Exception as exc:  # noqa: BLE001 - quota state is unknown
                        logger.warning(
                            "idle route reclamation could not prove quota headroom: %s",
                            type(exc).__name__,
                        )

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                time.sleep(min(next(sleeps), remaining))
        except Exception as exc:  # noqa: BLE001 - pressure cleanup fails closed
            logger.warning(
                "idle route reclamation could not prove sandbox deletion: %s",
                type(exc).__name__,
            )
            return False

    def reap_orphans(self) -> list[str]:
        """Measure orphan cleanup at the substrate seam for every backend."""

        deleted: list[str] = []
        observed_claims: set[str] = set()
        inventory: dict[RouteState, set[str]] = {state: set() for state in RouteState}
        error: Exception | None = None
        with operation_span(
            "curie.sandbox.cleanup",
            kind=SpanKind.INTERNAL,
            attributes={"service.name": "curie-worker", "operation": "cleanup"},
        ) as span:
            try:
                inventory = self._affinity.route_inventory()
                deleted, observed_claims = self._reap_orphans(inventory)
            except Exception as exc:
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "sandbox.cleanup.failed",
                    {"outcome": "failed", "error.class": type(exc).__name__},
                )
            else:
                span.add_event(
                    "sandbox.cleanup.completed",
                    {"outcome": "orphan-cleaned" if deleted else "observed"},
                )
        outcome = "failed" if error is not None else ("orphan-cleaned" if deleted else "observed")
        attributes = _sandbox_attributes("cleanup", outcome)
        record_metric("curie.sandbox.lifecycle", attributes=attributes)
        record_metric("curie.sandbox.cleanup", float(len(deleted)), attributes=attributes)
        if error is not None:
            raise error
        active_routes = inventory[RouteState.LIVE]
        suspended_routes = inventory[RouteState.SUSPENDED]
        _record_inventory(
            active=float(len(active_routes & observed_claims)),
            suspended=float(len(suspended_routes & observed_claims)),
        )
        record_metric(
            "curie.thread.route.active",
            float(len(active_routes)),
            attributes={
                "service.name": "curie-worker",
                "source": "worker",
                "outcome": "observed",
            },
        )
        return deleted

    def _reap_orphans(
        self, inventory: dict[RouteState, set[str]]
    ) -> tuple[list[str], set[str]]:
        """Delete substrate-managed claims that no live route references AND
        that are older than the bind-window grace.

        Routes expire from Valkey by TTL (idle threads); the corresponding
        claims are then orphans on the cluster. Runs from a periodic worker
        tick. Returns the deleted claim names plus the still-observed claim
        names so the caller can publish authoritative inventory without a
        second cluster or Valkey scan.

        "No route" alone does NOT mean litter. ``_claim_fresh`` writes the
        thread's route only after the claim binds a sandbox and that sandbox
        publishes a dial target, so a claim still inside that window is live
        and routeless at the same time. Reaping it deletes a running sandbox
        out from under a blocked creator, which then polls a claim that no
        longer exists until its deadline and reports a bind timeout. Age
        disambiguates: past the grace no creator can
        still be waiting, because ``_claim_fresh`` deletes its own claim on
        both failure paths.

        The grace is pinned to ``claim_timeout_seconds`` because that is the
        one budget every in-flight claim is created under: ``create_claim`` is
        called from exactly one place, ``_claim_fresh``, reached only from
        ``claim()`` and ``resume()``. A future second creation path with a
        different budget would silently fall outside this guard.

        The clock here is deliberately wall clock, not the ``time.monotonic``
        ``_claim_fresh`` measures its deadline with: ``created_at`` comes from
        another process on another node, and there is no shared monotonic
        epoch to compare it against. That mismatch is precisely what
        REAP_GRACE_MARGIN_SECONDS pads for; do not unify the two clocks.
        """

        live = set().union(*inventory.values())
        deleted: list[str] = []
        selector = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"
        grace = self._config.claim_timeout_seconds + REAP_GRACE_MARGIN_SECONDS
        cutoff = datetime.now(UTC) - timedelta(seconds=grace)
        claims = self._k8s.list_claims(label_selector=selector)
        for claim in claims:
            if claim.name in live:
                continue
            if claim.created_at is None:
                # Unknown age is never reaped, in the fail-safe direction. It
                # is also the one state that exempts a claim indefinitely
                # instead of merely delaying it, so it must announce itself:
                # an adapter bug or a parse defect that yields None for every
                # claim turns orphan reaping off entirely, and claims then
                # accumulate to ResourceQuota exhaustion with nothing anywhere
                # reporting it.
                logger.warning(
                    "sandbox claim %s reports no creation instant; its age is unknown "
                    "so it will not be reaped. Orphan reaping is disabled for every "
                    "claim the substrate client answers this way.",
                    claim.name,
                )
                continue
            if claim.created_at >= cutoff:
                # Inside the bind window: a creator may still be waiting on it.
                # The comparison is >=, so a claim exactly at the grace is
                # spared. Ties go to the creator; do not simplify this to >.
                continue
            self._k8s.delete_claim(
                claim.name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            )
            deleted.append(claim.name)
        observed = {claim.name for claim in claims} - set(deleted)
        return deleted, observed

    # -- internals --------------------------------------------------------------

    def _claim_fresh(
        self,
        thread_key: str,
        *,
        env: dict[str, str] | None,
        state: RouteState,
        session_id: str | None = None,
        history_ref: str | None = None,
        agent_name: str | None = None,
        workspace_repo: str | None = None,
        workspace_materialized_head: str | None = None,
        publication_visible_outcome_revision: int = 0,
        generation: int = 0,
        publish: bool = True,
        fresh_only: bool = False,
    ) -> SandboxHandle:
        config = self._config
        nonce = uuid.uuid4().hex[:6]
        name = config.claim_name_for(thread_key, nonce)
        thread_hash = name.rsplit("-", 1)[0].rsplit("-", 1)[-1]
        labels = {THREAD_HASH_LABEL: thread_hash}
        if agent_name:
            labels[AGENT_LABEL] = agent_name

        self._k8s.create_claim(
            name,
            pool=claim_warm_pool(config.warm_pool, env, agent_name, config.agent_pools),
            env=env,
            labels=labels,
        )
        deadline = time.monotonic() + config.claim_timeout_seconds
        try:
            sandbox_name = self._await_bound(name, deadline)
            bound = self._await_service_fqdn(sandbox_name, deadline)
        except Exception:
            self._k8s.delete_claim(
                name, request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S
            )
            raise

        handle = SandboxHandle(
            thread_key=thread_key,
            claim_name=name,
            sandbox_name=sandbox_name,
            namespace=config.namespace,
            service_fqdn=bound.service_fqdn or "",
            port=bound.port if bound.port is not None else config.runner_port,
            # The route must describe the runner that actually booted.  Bound
            # claims receive their authoritative identity in this exact env;
            # the explicit values remain fallbacks for lifecycle callers that
            # preserve identity without carrying those optional env entries.
            session_id=(env or {}).get(SESSION_ENV)
            or session_id
            or f"thread-{thread_hash}",
            history_ref=(env or {}).get(HISTORY_ENV) or history_ref,
            token=(env or {}).get(RUNNER_TOKEN_ENV, ""),
            workspace_repo=workspace_repo,
            workspace_materialized_head=workspace_materialized_head,
            publication_visible_outcome_revision=publication_visible_outcome_revision,
            generation=generation,
            max_turns=(env or {}).get(MAX_TURNS_ENV),
        )
        if not publish:
            return handle
        record = RouteRecord(handle=handle, state=state)
        for _ in range(3):
            if self._affinity.put_if_absent(thread_key, record, config.route_ttl_seconds):
                return handle
            # Lost the race: another worker recorded a route first. Adopt the
            # winner only if its sandbox is actually alive; a stale route
            # (dead sandbox) is evicted and the put retried.
            winner = self._affinity.get(thread_key)
            if winner is None:
                continue
            if winner.state is RouteState.SUSPENDED:
                self._k8s.delete_claim(
                    name, request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S
                )
                raise SuspendedThreadError(thread_key)
            sandbox = self._k8s.get_sandbox(
                winner.handle.sandbox_name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            )
            if sandbox is not None and sandbox.operating_mode == "Running":
                self._k8s.delete_claim(
                    name, request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S
                )
                if fresh_only:
                    # The winner never saw this claim's env (#2739).
                    raise RouteChangedError(thread_key)
                return winner.handle
            self._evict_stale(thread_key, winner)
        self._k8s.delete_claim(
            name, request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S
        )
        raise NoRouteError(f"could not record a route for {thread_key} after repeated races")

    def _evict_stale(self, thread_key: str, record: RouteRecord) -> None:
        """Retire a route whose sandbox is gone: delete its claim (idempotent)
        and drop the route, guarded so a fresher route is never deleted."""

        self._k8s.delete_claim(
            record.handle.claim_name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
        )
        self._affinity.delete_if_claim(thread_key, record.handle.claim_name)

    def _await_bound(self, claim_name: str, deadline: float) -> str:
        last_quota_rejection = None
        last_ready_condition: tuple[str | None, str | None] | None = None
        consecutive_quota = 0
        sleeps = _poll_sleeps(self._config)
        while time.monotonic() < deadline:
            claim = self._k8s.get_claim(
                claim_name,
                request_timeout_seconds=min(
                    _CONTROL_REQUEST_TIMEOUT_S,
                    max(0.001, deadline - time.monotonic()),
                ),
            )
            if claim is not None:
                last_quota_rejection = claim.quota_rejection
                if claim.quota_rejection is not None:
                    consecutive_quota += 1
                    # Two observations is the debounce: a single-poll blip can
                    # still bind (#1572 transient-clear), but a persisting
                    # ResourceQuota rejection is terminal now rather than after
                    # claim_timeout_seconds (#1534).
                    if consecutive_quota >= 2:
                        raise CapacityExhaustedError(claim.quota_rejection)
                else:
                    consecutive_quota = 0
                if claim.ready_reason is not None or claim.ready_message is not None:
                    last_ready_condition = (claim.ready_reason, claim.ready_message)
                if claim.ready and claim.sandbox_name:
                    return claim.sandbox_name
            # Clamped to the time left in the shared budget: an unclamped
            # backed-off sleep would overshoot the deadline by up to the cap and
            # steal that much from the serviceFQDN phase downstream.
            time.sleep(max(0.0, min(next(sleeps), deadline - time.monotonic())))
        if last_quota_rejection is not None:
            raise CapacityExhaustedError(last_quota_rejection)
        condition_detail = "no Ready condition was observed"
        if last_ready_condition is not None:
            reason, message = last_ready_condition
            condition_detail = f"last Ready condition had reason={reason!r} and message={message!r}"
        raise ClaimTimeoutError(
            f"claim {claim_name} not bound within {self._config.claim_timeout_seconds}s; "
            f"{condition_detail}."
        )

    def _await_service_fqdn(self, sandbox_name: str, deadline: float) -> SandboxView:
        sleeps = _poll_sleeps(self._config)
        while time.monotonic() < deadline:
            sandbox = self._k8s.get_sandbox(
                sandbox_name,
                request_timeout_seconds=min(
                    _CONTROL_REQUEST_TIMEOUT_S,
                    max(0.001, deadline - time.monotonic()),
                ),
            )
            if sandbox is not None and sandbox.service_fqdn:
                return sandbox
            time.sleep(max(0.0, min(next(sleeps), deadline - time.monotonic())))
        raise ClaimTimeoutError(
            f"sandbox {sandbox_name} has no serviceFQDN within "
            f"{self._config.claim_timeout_seconds}s (is spec.service true in the template?)"
        )
