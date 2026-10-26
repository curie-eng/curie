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
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from aci_protocol import BootEnv
from curie_telemetry import operation_span, record_metric
from curie_telemetry.redact import redact_text
from opentelemetry.trace import SpanKind, StatusCode

from ..binding import (
    BUNDLE_REF_ENV,
    CONNECTOR_CALLER_TOKEN_ENV,
    HISTORY_TOKEN_ENV,
    MAX_TURNS_ENV,
    RUNNER_TOKEN_ENV,
    SANDBOX_TOKEN_TTL_SECONDS,
    boot_token_facts,
)
from ..workitem_dispatch import TerminationObservation
from . import tool_daemon
from .affinity import AffinityStore
from .docker import DockerSandboxClient
from .k8s import KubernetesSandboxClient
from .types import (
    AGENT_LABEL,
    EXECUTOR_THREAD_KEY_PREFIX,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    THREAD_HASH_LABEL,
    TURN_PROGRESS_ELIGIBILITY_ENV,
    CapacityExhaustedError,
    ClaimNameConflict,
    ClaimTimeoutError,
    KubeTransientError,
    MissingAgentPoolError,
    NoRouteError,
    PressureScanResult,
    QuotaRejection,
    RepoRunnerPoolRefused,
    RouteChangedError,
    RouteRecord,
    RouteState,
    SandboxClient,
    SandboxHandle,
    SandboxTermination,
    SandboxView,
    SubstrateConfig,
    SuspendedThreadError,
    UnschedulableClaimError,
    agent_warm_pool_name,
    claim_warm_pool,
    has_connector_secrets,
    is_executor_thread_key,
    repo_runner_entry,
)

if TYPE_CHECKING:
    from ..repo_runner_pools import RepoRunnerPool
    from .daemon_config import ToolDaemonConfig

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
# After its first poll, a wait loop sends no further poll with less than this
# left before its deadline: a sub-second request timeout cannot get an answer
# from a loaded apiserver and only produces a misleading kube API error in place
# of the real timeout (#4181). The first poll is exempt, so a claim budget an
# operator configured below this floor still gets one look at the claim.
_MIN_POLL_BUDGET_S = 0.5
_GONE_READ_TIMEOUT_S = 1.0
_POD_TERMINATION_TIMEOUT_S = 2.0


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


def _refuse_executor_key(thread_key: str, operation: str) -> None:
    """Refuse an executor route on a path that cannot strip its template."""

    if is_executor_thread_key(thread_key):
        raise ValueError(
            f"{operation} refuses an {EXECUTOR_THREAD_KEY_PREFIX!r} route; an executor "
            "sandbox is claimed fresh and released, never carried over"
        )


def _refuse_unsecreted_repo_claim(
    runner: RepoRunnerPool | None,
    env: Mapping[str, str] | None,
    agent_name: str | None,
    connector_secret_pools: frozenset[str],
) -> None:
    """Refuse a connector-secret claim a repository pool cannot serve (#2943).

    The repository pool's template must carry the agent's connector secrets
    (rendered per agent and digest, ADR 0216 d5); the values are stripped from
    the claim env, so an agent without a secret pool is refused, as is an
    unnamed one, before any credential is minted or anything is written.
    """

    if runner is None:
        return
    if has_connector_secrets(env) and (not agent_name or agent_name not in connector_secret_pools):
        raise MissingAgentPoolError(agent_name or "(unnamed)", runner.pool)


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
        self._boot_credential_revoker: Callable[[str, str], bool] | None = None
        self._boot_credential_minter: Callable[[Mapping[str, str]], dict[str, str]] | None = None

    def set_boot_credential_revoker(self, revoker: Callable[[str, str], bool]) -> None:
        """Report a released boot credential. The kernel wires the API call."""

        self._boot_credential_revoker = revoker

    def set_boot_credential_minter(
        self, minter: Callable[[Mapping[str, str]], dict[str, str]]
    ) -> None:
        """Give each new claim its own boot credential. The kernel wires the signer.

        A retry claims again from the env whose credential the failed claim
        already released, and the API refuses a released credential.
        """

        self._boot_credential_minter = minter

    def _remember_claim_credential(self, claim_name: str, env: Mapping[str, str] | None) -> None:
        token = None
        if env is not None:
            token = env.get(HISTORY_TOKEN_ENV) or env.get(BootEnv.env_key("state_token"))
        agent, cred, _exp = boot_token_facts(token)
        if agent and cred:
            self._affinity.remember_claim_credential(
                claim_name, agent, cred, ttl_s=SANDBOX_TOKEN_TTL_SECONDS
            )

    def _report_boot_credential(
        self,
        *,
        claim_name: str | None = None,
        handle: SandboxHandle | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        """Report the boot credential, and keep the record until the API accepts it.

        A missing revoker leaves the record so a later reap can retry once the
        worker is wired. A failed report does the same. Expiry of the record
        matches the longest a token can still be valid.
        """

        agent: str | None = None
        cred: str | None = None
        stored: tuple[str, str] | None = None
        if claim_name is not None:
            stored = self._affinity.claim_credential(claim_name)
            if stored is not None:
                agent, cred = stored
        if not agent or not cred:
            if handle is not None and handle.state_credential_id and handle.state_credential_agent:
                agent, cred = handle.state_credential_agent, handle.state_credential_id
            else:
                token = None
                if env is not None:
                    token = env.get(HISTORY_TOKEN_ENV) or env.get(BootEnv.env_key("state_token"))
                agent, cred, _exp = boot_token_facts(token)
        if agent and cred and claim_name is not None and stored is None:
            self._affinity.remember_claim_credential(
                claim_name, agent, cred, ttl_s=SANDBOX_TOKEN_TTL_SECONDS
            )
        if not agent or not cred:
            return
        if claim_name is not None:
            self._affinity.mark_claim_credential_retiring(
                claim_name, ttl_s=SANDBOX_TOKEN_TTL_SECONDS
            )
        revoker = self._boot_credential_revoker
        if revoker is None:
            return
        try:
            accepted = revoker(agent, cred) is True
        except Exception:  # noqa: BLE001 - the record stays for a later retry
            logger.warning("could not report sandbox credential %s as released", cred)
            accepted = False
        if accepted and claim_name is not None:
            self._affinity.forget_claim_credential(claim_name)

    def _retire_claim(
        self,
        name: str,
        *,
        request_timeout_seconds: float,
        handle: SandboxHandle | None = None,
        env: Mapping[str, str] | None = None,
        daemon: bool = True,
    ) -> None:
        """Revoke the boot credential, delete the claim, then retire its daemon.

        Every claim delete uses this. The daemon (ADR 0217) is retired even when
        the claim delete raised, with its own ``request_timeout_seconds`` budget
        so a delete that timed out cannot starve it, and the claim's error is the
        one raised. ``daemon=False`` is a caller that retires the daemon itself.
        """

        try:
            self._report_boot_credential(claim_name=name, handle=handle, env=env)
            self._k8s.delete_claim(name, request_timeout_seconds=request_timeout_seconds)
        except Exception:
            if daemon:
                deadline = time.monotonic() + request_timeout_seconds
                self._retire_daemon_quietly(name, handle, deadline)
            raise
        if daemon:
            self._retire_daemon(name, handle, time.monotonic() + request_timeout_seconds)

    def _daemon_configs(self) -> dict[str, ToolDaemonConfig]:
        """The configured tool daemons by namespace; none on the Docker substrate."""

        if isinstance(self._k8s, DockerSandboxClient):
            return {}
        return {
            entry.daemon.namespace: entry.daemon
            for entry in self._config.repo_runner_pools.values()
            if entry.docker and entry.daemon is not None
        }

    def _retire_daemon(self, name: str, handle: SandboxHandle | None, deadline: float) -> None:
        """Delete claim ``name``'s daemon objects, then forget its marker and credential.

        A claim has a daemon when its handle says so or, without one, when the
        Valkey marker does; an unreadable marker means no explicit call, and
        the orphan sweep remains the backstop.
        """

        configs = self._daemon_configs()
        if not configs:
            return
        try:
            namespace = self._affinity.daemon_namespace(name)
        except Exception as exc:  # noqa: BLE001 - unknown, the sweep covers it
            logger.warning("tool daemon marker of %s unreadable (%s)", name, type(exc).__name__)
            namespace = None
        if namespace is None and handle is not None and handle.runner_docker:
            found = repo_runner_entry(handle.workspace_repo, self._config.repo_runner_pools)
            daemon = None if found is None else found[1].daemon
            namespace = None if daemon is None else daemon.namespace
        config = None if namespace is None else configs.get(namespace)
        if config is None:
            return
        tool_daemon.retire(self._k8s, config, name, deadline=deadline)
        self._affinity.forget_daemon(name)

    def _retire_daemon_quietly(
        self, name: str, handle: SandboxHandle | None, deadline: float
    ) -> None:
        """``_retire_daemon`` beside a primary error the caller re-raises."""

        try:
            self._retire_daemon(name, handle, deadline)
        except Exception as exc:  # noqa: BLE001 - the primary error wins
            logger.warning("tool daemon of claim %s not retired (%s)", name, type(exc).__name__)

    def _retire_claim_quietly(
        self,
        name: str,
        *,
        handle: SandboxHandle | None = None,
        env: Mapping[str, str] | None = None,
        daemon: bool = True,
    ) -> None:
        """``_retire_claim`` beside a primary error the caller re-raises."""

        try:
            self._retire_claim(
                name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                handle=handle,
                env=env,
                daemon=daemon,
            )
        except Exception as exc:  # noqa: BLE001 - the primary error wins
            logger.warning("claim %s not retired (%s)", name, type(exc).__name__)

    def _retry_unreported_credentials(self) -> None:
        """Reap retries a report whose claim is already gone."""

        for claim_name, _agent, _cred in self._affinity.iter_claim_credentials():
            if not self._affinity.claim_credential_retiring(claim_name):
                continue
            try:
                still_there = self._k8s.get_claim(
                    claim_name, request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S
                )
            except Exception:  # noqa: BLE001 - a lookup failure is not proof the claim is gone
                logger.warning(
                    "could not look up claim %s while retrying its credential", claim_name
                )
                continue
            if still_there is None:
                self._report_boot_credential(claim_name=claim_name)

    def pod_termination(
        self, handle: SandboxHandle, *, since: datetime
    ) -> SandboxTermination | None:
        """Read one bounded, substrate neutral diagnosis for a dropped stream."""

        if handle.namespace != self._config.namespace:
            return None
        return self._k8s.pod_termination(
            handle.sandbox_name,
            since=since,
            request_timeout_seconds=_POD_TERMINATION_TIMEOUT_S,
        )

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
        runner_resources: dict[str, Any] | None = None,
        caller_run: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
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

        ``executor_secret_names`` marks an executor claim (@spec
        ACTION-EXECUTOR-5): ``None`` is an ordinary claim, a set (possibly
        empty) names the connector secrets the target connector's headers
        expand, and the claim runs from a stripped per-claim template. It must
        agree with the ``action-exec:`` thread key prefix, which is what
        pressure filtering keys on; a mismatch raises ``ValueError`` before any
        route read or Kubernetes write.
        """

        if (executor_secret_names is not None) != is_executor_thread_key(thread_key):
            raise ValueError(
                "executor_secret_names must be given exactly for an "
                f"{EXECUTOR_THREAD_KEY_PREFIX!r} thread key"
            )
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
                # A named repository's refusal is decided before any existing
                # route is evicted, so a refusal leaves the thread as it was.
                preflight = self._preflight_repo_runner(workspace_repo, env, agent_name)
                record = self._affinity.get(thread_key)
                if record is not None:
                    if record.state is RouteState.SUSPENDED:
                        raise SuspendedThreadError(thread_key)
                    sandbox = self._k8s.get_sandbox(
                        record.handle.sandbox_name,
                        request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                    )
                    # A refusal raised by the policy check propagates with the
                    # route untouched: a refusal is a decision, not a fault.
                    if (
                        sandbox is not None
                        and sandbox.operating_mode == "Running"
                        and self._route_matches_runner(
                            record.handle,
                            workspace_repo,
                            agent_name=agent_name,
                            desired=preflight,
                        )
                    ):
                        if fresh_only:
                            raise RouteChangedError(thread_key)
                        self._affinity.touch(thread_key, self._config.route_ttl_seconds)
                        handle = record.handle
                        outcome = "reused"
                    else:
                        # A stopped sandbox, or a runner image its repository's
                        # policy no longer names (ADR 0216).
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
                        publication_visible_outcome_revision=(publication_visible_outcome_revision),
                        fresh_only=fresh_only,
                        runner_resources=runner_resources,
                        caller_run=caller_run,
                        executor_secret_names=executor_secret_names,
                        repo_runner=preflight,
                    )
                    outcome = "claimed"
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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

    def touch_live(self, thread_key: str, claim_name: str) -> bool:
        """Refresh the route TTL while a turn streams on ``claim_name`` (#3188).

        Only a LIVE route that still names ``claim_name`` is refreshed: a
        suspended route keeps its own longer TTL, and a route a handoff replaced
        belongs to someone else. Returns whether the TTL was refreshed.
        """

        return self._affinity.touch_if_live_claim(
            thread_key, claim_name, self._config.route_ttl_seconds
        )

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
        # Executor routes never reach the kernel's reclamation (@spec
        # ACTION-EXECUTOR-5): the loop releases its own sandbox on every path.
        return PressureScanResult(
            tuple(
                candidate
                for candidate in result.candidates
                if candidate.record.handle.namespace == self._config.namespace
                and not is_executor_thread_key(candidate.thread_key)
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
        if claim is None or not claim.ready or claim.sandbox_name != record.handle.sandbox_name:
            self._evict_stale(thread_key, record)
            return None

        sandbox = self._k8s.get_sandbox(
            record.handle.sandbox_name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
        )
        if sandbox is None or not sandbox.ready or sandbox.operating_mode != "Running":
            self._evict_stale(thread_key, record)
            return None

        # A runner whose image its repository's entry no longer names is
        # retired, so the caller's fresh claim boots the current one (ADR 0216).
        if not self._route_matches_runner(record.handle, None):
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
        runner_resources: dict[str, Any] | None = None,
        caller_run: str | None = None,
    ) -> SandboxHandle:
        """Cold create a runner, then CAS it over one retained route.

        The old route remains authoritative while the candidate binds. Losing
        the claim+generation fence deletes only the unexposed candidate. After
        a successful swap the old claim is cleanup-only; a failed deletion is
        intentionally recoverable by the ordinary orphan reaper.

        An executor route is refused with ``ValueError`` before any write: its
        replacement would boot from the unstripped pool template (@spec
        ACTION-EXECUTOR-5).
        """

        _refuse_executor_key(thread_key, "handoff")
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
            runner_resources=runner_resources,
            caller_run=caller_run,
        )
        try:
            if validate_candidate is not None:
                validate_candidate(candidate)
        except Exception:
            # The candidate is ready but still unrouted. Refusal retires only
            # that unexposed claim; the old route remains authoritative until
            # the generation CAS below succeeds.
            self._retire_claim_quietly(candidate.claim_name, handle=candidate)
            raise
        record = RouteRecord(handle=candidate, state=RouteState.LIVE)
        if not self._affinity.replace_if_generation(
            thread_key,
            expected_claim=expected.claim_name,
            expected_generation=expected.generation,
            record=record,
            ttl_seconds=self._config.route_ttl_seconds,
        ):
            self._retire_claim(
                candidate.claim_name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                handle=candidate,
            )
            raise NoRouteError(f"late handoff lost its route fence for {thread_key}")
        try:
            self._retire_claim(
                expected.claim_name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                handle=expected,
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
                # A suspended route keeps its claim but not its daemon (ADR 0217);
                # resume provisions a fresh one with the replacement claim.
                # The retirement budget starts once the patch returns or raises.
                try:
                    self._k8s.set_sandbox_mode(record.handle.sandbox_name, "Suspended")
                    self._affinity.mark_suspended(
                        thread_key, history_ref, self._config.suspended_route_ttl_seconds
                    )
                except Exception:
                    retire_by = time.monotonic() + _CONTROL_REQUEST_TIMEOUT_S
                    self._retire_daemon_quietly(record.handle.claim_name, record.handle, retire_by)
                    raise
                retire_by = time.monotonic() + _CONTROL_REQUEST_TIMEOUT_S
                self._retire_daemon(record.handle.claim_name, record.handle, retire_by)
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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
        runner_resources: dict[str, Any] | None = None,
        caller_run: str | None = None,
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

        An executor route is refused with ``ValueError`` before any route read
        or write: executor routes are never suspended, and a replacement would
        boot from the unstripped pool template (@spec ACTION-EXECUTOR-5).
        """

        _refuse_executor_key(thread_key, "resume")
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
                # Every refusal is decided before the suspended claim is retired.
                resume_repo = workspace_repo or old.workspace_repo
                runner = self._preflight_repo_runner(resume_repo, boot, agent_name)

                self._retire_claim(
                    old.claim_name,
                    request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                    handle=old,
                )
                self._affinity.delete_if_claim(thread_key, old.claim_name)
                handle = self._claim_fresh(
                    thread_key,
                    env=boot,
                    state=RouteState.LIVE,
                    session_id=old.session_id,
                    history_ref=old.history_ref,
                    agent_name=agent_name,
                    workspace_repo=resume_repo,
                    workspace_materialized_head=(
                        workspace_materialized_head or old.workspace_materialized_head
                    ),
                    publication_visible_outcome_revision=(
                        publication_visible_outcome_revision
                        if publication_visible_outcome_revision is not None
                        else old.publication_visible_outcome_revision
                    ),
                    generation=old.generation + 1,
                    runner_resources=runner_resources,
                    caller_run=caller_run,
                    repo_runner=runner,
                )
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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
                    self._retire_claim(
                        claim_name,
                        request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                        handle=record.handle,
                    )
                    self._affinity.delete_if_claim(thread_key, claim_name)
                    released = True
                    if wait_gone:
                        self._await_quota_freed(claim_name, sandbox_name)
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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

    def executor_routes(self) -> dict[str, SandboxHandle]:
        """The handle of every executor route (``action-exec:<id>``), by thread key.

        @spec AUTOMATED-REMEDIATION-12: read by the executor loop to release a
        crashed holder's sandbox once the API has ended its execution; the
        release itself is ``release_claim`` of exactly the handle read here.
        """

        return {key: record.handle for key, record in self._affinity.executor_routes().items()}

    def release_claim(self, thread_key: str, handle: SandboxHandle) -> bool:
        """Release exactly the claim ``handle`` names (@spec ACTION-EXECUTOR-5).

        For a holder that may have been fenced out: its own claim is retired
        (an idempotent delete) and the route is dropped only while it still
        names that claim, so a later holder's live sandbox on the same thread
        key is never touched. True when the route was dropped.
        """

        self._retire_claim(
            handle.claim_name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            handle=handle,
        )
        dropped = self._affinity.delete_if_claim(thread_key, handle.claim_name)
        record_metric(
            "curie.sandbox.lifecycle",
            attributes=_sandbox_attributes("release", "released" if dropped else "observed"),
        )
        return dropped

    def release_if_claim(self, thread_key: str, claim_name: str) -> bool:
        """Release the thread's session only while its route names ``claim_name``.

        A WorkItem run releases the claim it recorded, never whatever the
        thread holds now (#4331). When the route is absent or names another
        claim, nothing is deleted: no Kubernetes delete, no route delete, and
        the call returns False. When it names ``claim_name``, the claim is
        retired and the route dropped exactly as ``release`` does, and the call
        returns True. The caller holds the cross-worker thread lock, which a
        successor's claim also holds, so the read and the delete cannot
        interleave with it.
        """

        started = time.monotonic()
        released = False
        current_claim: str | None = None
        error: Exception | None = None
        with operation_span(
            "curie.sandbox.release",
            kind=SpanKind.INTERNAL,
            attributes={"service.name": "curie-worker", "operation": "release_if_claim"},
        ) as span:
            try:
                record = self._affinity.get(thread_key)
                current_claim = record.handle.claim_name if record is not None else None
                if record is not None and current_claim == claim_name:
                    self._retire_claim(
                        claim_name,
                        request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                        handle=record.handle,
                    )
                    self._affinity.delete_if_claim(thread_key, claim_name)
                    released = True
            except Exception as exc:  # noqa: BLE001 - failure boundary
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
                    {"outcome": "released" if released else "fenced"},
                )
        if error is None and not released:
            logger.info(
                "sandbox release fenced: thread %s route names claim %s, run owns claim %s;"
                " leaving the route",
                thread_key,
                current_claim,
                claim_name,
            )
        outcome = "failed" if error is not None else ("released" if released else "fenced")
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
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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
        # The route may name a replacement claim the SQL row has not heard
        # about (ADR 0178: a token that would outlive the deadline is handed
        # off before the turn). That live claim is part of this thread.
        if record is not None:
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
                self._retire_claim(
                    name,
                    request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                    handle=record.handle if record is not None and name == affinity_claim else None,
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
                claim_gone = (
                    self._k8s.get_claim(
                        claim_name,
                        request_timeout_seconds=min(
                            _GONE_READ_TIMEOUT_S,
                            max(0.001, deadline - time.monotonic()),
                        ),
                    )
                    is None
                )
            except Exception as exc:  # noqa: BLE001 - the gone wait is soft
                logger.warning(
                    "release gone wait could not read claim: %s",
                    type(exc).__name__,
                )

            sandbox_gone = False
            try:
                sandbox_gone = (
                    self._k8s.get_sandbox(
                        sandbox_name,
                        request_timeout_seconds=min(
                            _GONE_READ_TIMEOUT_S,
                            max(0.001, deadline - time.monotonic()),
                        ),
                    )
                    is None
                )
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
            self._retire_claim(
                record.handle.claim_name,
                request_timeout_seconds=min(_CONTROL_REQUEST_TIMEOUT_S, remaining),
                handle=record.handle,
            )
            sleeps = _poll_sleeps(self._config)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                claim_gone = sandbox_gone = False
                try:
                    claim_gone = (
                        self._k8s.get_claim(
                            record.handle.claim_name,
                            request_timeout_seconds=min(_GONE_READ_TIMEOUT_S, remaining),
                        )
                        is None
                    )

                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    sandbox_gone = (
                        self._k8s.get_sandbox(
                            record.handle.sandbox_name,
                            request_timeout_seconds=min(_GONE_READ_TIMEOUT_S, remaining),
                        )
                        is None
                    )
                except KubeTransientError as exc:
                    # A failed read proves neither presence nor absence. Keep
                    # polling within the original cleanup window, and require
                    # both real absence observations before the quota proof.
                    logger.warning(
                        "idle route reclamation skipped a transient disappearance read: %s",
                        type(exc).__name__,
                    )
                if claim_gone and sandbox_gone:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    try:
                        if self._k8s.quota_has_headroom(
                            rejection,
                            request_timeout_seconds=min(_GONE_READ_TIMEOUT_S, remaining),
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

    def claim_bundle_ref(self, claim_name: str) -> str | None:
        """The ``CURIE_BUNDLE_REF`` a claim was created with, or None (ADR 0100).

        Sync control plane read, like the rest of this seam."""

        view = self._k8s.get_claim(claim_name, request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S)
        return None if view is None else view.bundle_ref

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
                self._retry_unreported_credentials()
                inventory = self._affinity.route_inventory()
                deleted, observed_claims = self._reap_orphans(inventory)
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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

    def _reap_orphans(self, inventory: dict[RouteState, set[str]]) -> tuple[list[str], set[str]]:
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
            self._retire_claim(
                claim.name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            )
            deleted.append(claim.name)
        observed = {claim.name for claim in claims} - set(deleted)
        # A claim carrying scoped tokens owns a per-claim template (and through
        # it the token Secret and pool) only once ``create_claim`` patches the
        # template's ownerReference to the claim (#3842). A worker crash
        # between creating that template and the patch leaves it owned by
        # nothing, so garbage collection never removes it or its Secret. Sweep
        # those with the same cutoff as claims, keeping every claim still
        # observed. Best effort: a failed sweep must not cost claim reaping or
        # the inventory metric, and only the exception class is logged because
        # the API error body can carry object detail.
        try:
            self._k8s.reap_claim_templates(keep=observed, created_before=cutoff)
        except Exception as exc:  # noqa: BLE001 - a failed sweep retries next tick
            logger.warning(
                "sandbox claim-template sweep failed (%s); orphaned per-claim "
                "templates are retried on the next tick",
                type(exc).__name__,
            )
        self._sweep_daemons(keep=observed)
        return deleted, observed

    def _sweep_daemons(self, *, keep: set[str]) -> None:
        """Retire daemons a crash left without a claim (ADR 0217). Best effort.

        Runs only after a successful claim inventory, so a failed one never
        reads as "no claims". A daemon is swept once strictly older than the
        claim timeout, the longest a creator can still be provisioning it.
        """

        def claim_exists(claim: str) -> bool:
            view = self._k8s.get_claim(claim, request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S)
            return view is not None

        for config in self._daemon_configs().values():
            try:
                retired = tool_daemon.sweep_orphans(
                    self._k8s,
                    config,
                    keep=keep,
                    max_age_s=self._config.claim_timeout_seconds,
                    now=datetime.now(UTC),
                    claim_exists=claim_exists,
                )
                for claim in retired:
                    self._affinity.forget_daemon(claim)
            except Exception as exc:  # noqa: BLE001 - a failed sweep retries next tick
                logger.warning("tool daemon sweep failed (%s)", type(exc).__name__)

    # -- internals --------------------------------------------------------------

    def _existing_agent_pool(self, base_pool: str, agent_name: str | None) -> str | None:
        """The derived per-agent warm pool when the cluster already has it."""

        if not agent_name:
            return None
        derived = agent_warm_pool_name(base_pool, agent_name)
        if derived == base_pool:
            return None
        probe = getattr(self._k8s, "warm_pool_exists", None)
        if not callable(probe):
            return None
        try:
            present = bool(probe(derived))
        except Exception:  # noqa: BLE001 - an unreadable pool must not fail the claim
            logger.warning(
                "could not read SandboxWarmPool %s; the claim keeps the chart pool choice",
                derived,
            )
            return None
        return derived if present else None

    def _repo_runner(
        self, workspace_repo: str | None, *, agent_name: str | None = None
    ) -> RepoRunnerPool | None:
        """The repository's runner pool entry, checked usable, or None (ADR 0216).

        None on the Docker substrate (no warm pools) and for a repository
        without an entry, which keeps today's agent pool routing. An entry
        built on another platform runner, or whose pool was not rendered,
        raises RepoRunnerPoolRefused; an unreadable pool is transient.
        """

        if isinstance(self._k8s, DockerSandboxClient):
            return None
        found = repo_runner_entry(workspace_repo, self._config.repo_runner_pools)
        if found is None:
            return None
        repo, entry = found
        platform = self._config.platform_runner_digest
        if platform is None or platform != entry.base:
            raise RepoRunnerPoolRefused(
                repo=repo,
                pool=entry.pool,
                reason="stale_base",
                agent_name=agent_name,
                base=entry.base,
                platform_digest=platform,
            )
        probe = getattr(self._k8s, "warm_pool_exists", None)
        if callable(probe):
            try:
                present = bool(probe(entry.pool))
            except Exception as exc:
                # Only the class: an API error body can carry object detail.
                logger.warning(
                    "could not read SandboxWarmPool %s (%s)", entry.pool, type(exc).__name__
                )
                raise KubeTransientError(f"could not read SandboxWarmPool {entry.pool}") from exc
            if not present:
                raise RepoRunnerPoolRefused(
                    repo=repo, pool=entry.pool, reason="missing_pool", agent_name=agent_name
                )
        return entry

    def _preflight_repo_runner(
        self,
        workspace_repo: str | None,
        env: Mapping[str, str] | None,
        agent_name: str | None,
    ) -> RepoRunnerPool | None:
        """The repository's checked runner pool entry, with its secret refusal.

        Raises every refusal ``_repo_runner`` and the connector secret guard
        decide, so a caller runs it before any eviction, retirement, or
        credential mint.
        """

        runner = self._repo_runner(workspace_repo, agent_name=agent_name)
        _refuse_unsecreted_repo_claim(runner, env, agent_name, self._config.connector_secret_pools)
        return runner

    def _route_matches_runner(
        self,
        handle: SandboxHandle,
        workspace_repo: str | None,
        *,
        agent_name: str | None = None,
        desired: RepoRunnerPool | None = None,
    ) -> bool:
        """Whether a retained route still runs the image its repository needs.

        A caller naming a repository never adopts a route prepared for another
        one (or for none). The desired entry is the one for ``workspace_repo``,
        or the route's own repository when the caller names none. With no
        entry on either side the image matches, so an installation without
        repository pools reuses as before. Docker has no repository pools.
        ``desired`` is the caller's already checked entry for ``workspace_repo``;
        None recomputes it, which for a repository without an entry is
        deterministic and probes nothing.
        """

        if isinstance(self._k8s, DockerSandboxClient):
            return True
        if workspace_repo is not None and (
            handle.workspace_repo is None
            or handle.workspace_repo.casefold() != workspace_repo.casefold()
        ):
            return False
        if desired is not None and workspace_repo is not None:
            entry: RepoRunnerPool | None = desired
        else:
            repo = workspace_repo if workspace_repo is not None else handle.workspace_repo
            entry = self._repo_runner(repo, agent_name=agent_name)
        wanted = (None, False) if entry is None else (entry.digests, entry.docker)
        if (handle.runner_digests, handle.runner_docker) != wanted:
            return False
        return self._retained_runner_healthy(handle, entry)

    def _retained_runner_healthy(self, handle: SandboxHandle, entry: RepoRunnerPool | None) -> bool:
        """Whether a retained repository runner may serve another turn.

        A Docker-enabled route is reused only after its daemon answers an
        authenticated ping with the claim's own certificate within
        ``pingTimeoutSeconds`` (ADR 0217). It fails closed: a missing
        credential, Pod or answer replaces the route. Other routes issue no
        daemon call.
        """

        if entry is None or entry.daemon is None or not handle.runner_docker:
            return True
        daemon = entry.daemon
        try:
            files = self._affinity.daemon_credential(handle.claim_name)
        except Exception as exc:  # noqa: BLE001 - unknown is not healthy
            logger.warning("tool daemon credential unreadable (%s)", type(exc).__name__)
            return False
        if files is None:
            return False
        deadline = time.monotonic() + daemon.ping_timeout_seconds
        # Service, policy and ping all share one pingTimeoutSeconds budget.
        if not tool_daemon.endpoint_present(
            self._k8s, daemon, handle.claim_name, deadline=deadline
        ):
            return False
        host = tool_daemon.service_host(handle.claim_name, daemon.namespace)
        return tool_daemon.ping(
            self._k8s,
            daemon.namespace,
            handle.claim_name,
            daemon.port,
            host,
            files,
            deadline - time.monotonic(),
        )

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
        runner_resources: dict[str, Any] | None = None,
        caller_run: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
        repo_runner: RepoRunnerPool | None = None,
    ) -> SandboxHandle:
        # ``repo_runner`` is a caller's preflight of ``workspace_repo``'s entry,
        # refusals already decided. None recomputes it: a repository without an
        # entry resolves to None again with no probe.
        config = self._config
        nonce = uuid.uuid4().hex[:6]
        name = config.claim_name_for(thread_key, nonce)
        thread_hash = name.rsplit("-", 1)[0].rsplit("-", 1)[-1]
        labels = {THREAD_HASH_LABEL: thread_hash}
        if agent_name:
            labels[AGENT_LABEL] = agent_name

        # Docker has no warm pools and passes connector secrets directly to
        # the runner. The rendered pool check applies only to Kubernetes.
        # A repository bound to a runner pool (ADR 0216) claims that pool,
        # checked before any credential is minted so a refusal has no side
        # effects. A repository without one routes exactly as before.
        runner = (
            repo_runner
            if repo_runner is not None
            else self._preflight_repo_runner(workspace_repo, env, agent_name)
        )
        if isinstance(self._k8s, DockerSandboxClient):
            pool = config.warm_pool
        elif runner is not None:
            pool = runner.pool
        else:
            pool = claim_warm_pool(
                config.warm_pool,
                env,
                agent_name,
                config.agent_pools,
                config.connector_secret_pools,
            )
            # A per-agent pool cloned after a hosted connector deploy is usable
            # even when the chart did not list the agent. Absence keeps the
            # choice above, including the generic pool and the secret refusal.
            if pool == config.warm_pool:
                derived = self._existing_agent_pool(config.warm_pool, agent_name)
                if derived is not None:
                    pool = derived
        if env is not None and self._boot_credential_minter is not None:
            env = self._boot_credential_minter(env)
        self._remember_claim_credential(name, env)
        # Passed only for an executor claim, so a client that predates the
        # keyword keeps serving ordinary claims unchanged.
        executor_kwargs: dict[str, Any] = (
            {"executor_secret_names": executor_secret_names}
            if executor_secret_names is not None
            else {}
        )
        # One budget for daemon start, claim create, bind, serviceFQDN and the
        # daemon's first authenticated ping (ADR 0217).
        deadline = time.monotonic() + config.claim_timeout_seconds
        daemon = runner.daemon if runner is not None and runner.docker else None
        provisioned: tool_daemon.Provisioned | None = None
        marked = remembered = created = False
        try:
            if daemon is not None:
                # Associated before the first object exists, so even a crash
                # mid-provision leaves the claim's daemon findable.
                marked = self._affinity.mark_daemon(name, daemon.namespace)
                provisioned = tool_daemon.provision(
                    self._k8s,
                    daemon,
                    claim=name,
                    release_namespace=config.namespace,
                    egress_class=self._egress_class(workspace_repo, runner),
                    deadline=deadline,
                )
                self._affinity.remember_daemon_credential(
                    name, provisioned.mount.files, ttl_s=daemon.max_claim_lifetime_seconds
                )
                remembered = True
            # From here this attempt owns the claim name, even when the create
            # raises after the API server stored it.
            created = True
            self._k8s.create_claim(
                name,
                pool=pool,
                env=env,
                labels=labels,
                runner_resources=runner_resources,
                agent_name=agent_name,
                deadline=deadline,
                tool_daemon=None if provisioned is None else provisioned.mount,
                **executor_kwargs,
            )
            sandbox_name = self._await_bound(name, deadline)
            bound = self._await_service_fqdn(sandbox_name, deadline)
            if provisioned is not None:
                tool_daemon.await_ready(self._k8s, provisioned, deadline, _poll_sleeps(config))
        except BaseException as exc:  # noqa: BLE001 - retire owned resources on creator interruption
            # A 409 means the name is someone else's claim: never retire it.
            if created and not isinstance(exc, ClaimNameConflict):
                self._retire_claim_quietly(name, env=env, daemon=False)
            # Only what this attempt created goes: its objects by uid, and the
            # marker and credential only if this attempt wrote them.
            try:
                if provisioned is None or tool_daemon.rollback(
                    self._k8s, provisioned.config.namespace, provisioned.created
                ):
                    self._affinity.forget_daemon(name, marker=marked, credential=remembered)
            except Exception as cleanup:  # noqa: BLE001 - the primary error wins
                logger.warning("tool daemon of %s not forgotten (%s)", name, type(cleanup).__name__)
            raise

        credential_agent, credential_id, credential_exp = boot_token_facts(
            (env or {}).get(HISTORY_TOKEN_ENV)
        )
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
            session_id=(env or {}).get(SESSION_ENV) or session_id or f"thread-{thread_hash}",
            history_ref=(env or {}).get(HISTORY_ENV) or history_ref,
            token=(env or {}).get(RUNNER_TOKEN_ENV, ""),
            workspace_repo=workspace_repo,
            workspace_materialized_head=workspace_materialized_head,
            publication_visible_outcome_revision=publication_visible_outcome_revision,
            generation=generation,
            max_turns=(env or {}).get(MAX_TURNS_ENV),
            carries_caller_token=CONNECTOR_CALLER_TOKEN_ENV in (env or {}),
            carries_turn_progress=TURN_PROGRESS_ELIGIBILITY_ENV in (env or {}),
            caller_run=caller_run,
            state_credential_agent=credential_agent,
            state_credential_id=credential_id,
            state_token_exp=credential_exp,
            bundle_ref=(env or {}).get(BUNDLE_REF_ENV),
            runner_digests=None if runner is None else runner.digests,
            runner_docker=False if runner is None else runner.docker,
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
                self._retire_claim(
                    name,
                    request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                    handle=handle,
                )
                raise SuspendedThreadError(thread_key)
            sandbox = self._k8s.get_sandbox(
                winner.handle.sandbox_name,
                request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            )
            # A winner running an image this repository's policy does not
            # name is evicted like a dead one (ADR 0216).
            try:
                adoptable = (
                    sandbox is not None
                    and sandbox.operating_mode == "Running"
                    and self._route_matches_runner(
                        winner.handle,
                        workspace_repo,
                        agent_name=agent_name,
                        desired=runner,
                    )
                )
            except Exception:
                self._retire_claim_quietly(name, handle=handle)
                raise
            if adoptable:
                self._retire_claim(
                    name,
                    request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
                    handle=handle,
                )
                if fresh_only:
                    # The winner never saw this claim's env (#2739).
                    raise RouteChangedError(thread_key)
                return winner.handle
            self._evict_stale(thread_key, winner)
        self._retire_claim(
            name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            handle=handle,
        )
        raise NoRouteError(f"could not record a route for {thread_key} after repeated races")

    def _egress_class(self, workspace_repo: str | None, runner: RepoRunnerPool | None) -> str:
        """The chart's egress class for the daemon Pod: ``public``, or the
        allowlist of the repository's binding path (``repo-<sha256[:16]>``)."""

        found = repo_runner_entry(workspace_repo, self._config.repo_runner_pools)
        if runner is None or not runner.egress or found is None:
            return "public"
        return "repo-" + hashlib.sha256(found[0].encode()).hexdigest()[:16]

    def _evict_stale(self, thread_key: str, record: RouteRecord) -> None:
        """Retire a route whose sandbox is gone: delete its claim (idempotent)
        and drop the route, guarded so a fresher route is never deleted."""

        self._retire_claim(
            record.handle.claim_name,
            request_timeout_seconds=_CONTROL_REQUEST_TIMEOUT_S,
            handle=record.handle,
        )
        self._affinity.delete_if_claim(thread_key, record.handle.claim_name)

    def _await_bound(self, claim_name: str, deadline: float) -> str:
        last_quota_rejection = None
        last_ready_condition: tuple[str | None, str | None] | None = None
        consecutive_quota = 0
        last_unschedulable: str | None = None
        last_pod_name = claim_name
        sleeps = _poll_sleeps(self._config)
        polled = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or (polled and remaining < _MIN_POLL_BUDGET_S):
                break
            polled = True
            try:
                claim = self._k8s.get_claim(
                    claim_name,
                    request_timeout_seconds=min(_CONTROL_REQUEST_TIMEOUT_S, remaining),
                )
            except KubeTransientError as exc:
                # No observation this poll: the debounce and the last Ready
                # condition stay as the last real read left them, so the final
                # timeout still reports what the claim was actually waiting on.
                logger.info("transient read of claim %s: %s", claim_name, exc)
                claim = None
            if claim is not None:
                last_pod_name = claim.sandbox_name or claim_name
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
                if claim.quota_rejection is None:
                    # The latest read wins: a pod placed after an early
                    # Unschedulable is a slow start, not missing capacity.
                    # Cold-path pods carry the claim's name until the claim
                    # reports its sandbox (#3169).
                    last_unschedulable = self._k8s.pod_unschedulable(
                        claim.sandbox_name or claim_name,
                        request_timeout_seconds=min(
                            _CONTROL_REQUEST_TIMEOUT_S,
                            max(0.001, deadline - time.monotonic()),
                        ),
                    )
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
        if last_unschedulable is not None:
            raise UnschedulableClaimError(
                f"claim {claim_name} not bound within {self._config.claim_timeout_seconds}s; "
                f"its pod is Unschedulable: {last_unschedulable}"
            )
        if isinstance(self._k8s, KubernetesSandboxClient):
            tail = self._k8s.pod_log_tail(last_pod_name, request_timeout_seconds=5.0)
            if tail:
                logger.warning(
                    "runner log tail for claim %s pod %s:\n%s",
                    claim_name,
                    last_pod_name,
                    redact_text(tail),
                )
        raise ClaimTimeoutError(
            f"claim {claim_name} not bound within {self._config.claim_timeout_seconds}s; "
            f"{condition_detail}."
        )

    def _await_service_fqdn(self, sandbox_name: str, deadline: float) -> SandboxView:
        sleeps = _poll_sleeps(self._config)
        polled = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or (polled and remaining < _MIN_POLL_BUDGET_S):
                break
            polled = True
            try:
                sandbox = self._k8s.get_sandbox(
                    sandbox_name,
                    request_timeout_seconds=min(_CONTROL_REQUEST_TIMEOUT_S, remaining),
                )
            except KubeTransientError as exc:
                logger.info("transient read of sandbox %s: %s", sandbox_name, exc)
                sandbox = None
            if sandbox is not None and sandbox.service_fqdn:
                return sandbox
            time.sleep(max(0.0, min(next(sleeps), deadline - time.monotonic())))
        raise ClaimTimeoutError(
            f"sandbox {sandbox_name} has no serviceFQDN within "
            f"{self._config.claim_timeout_seconds}s (is spec.service true in the template?)"
        )
