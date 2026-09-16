"""F3 eval-stream consumer: run eval suites requested on the curie:evals stream.

The K1-API producer XADDs one stream field ``payload`` holding an ``EvalJob``
JSON (the dispatcher seam convention). This consumer runs a distinct consumer
group (``curie-eval-workers``) so it does not compete with the runs consumer.
For each entry it:

  1. fetches the version's immutable bundle from RustFS by ``bundle_ref`` and loads
     the suite from the bundle's own ``evals/cases.json`` (the same shape the CLI's
     ``curie skill eval`` reads); an optional ``evals/trajectory.json`` selects
     deterministic trajectory scoring above that frozen format; the ``suite``
     field names it and tags Langfuse;
  2. runs the suite against the runner: ``target_url`` if given (the dev/test
     shortcut), otherwise provisions a sandbox for the version via the G1
     substrate (the same boot env F2 uses) and tears it down in a finally;
  3. records per-case scores to Langfuse (keyed by version, the shape the matrix
     endpoint reads) and POSTs a summary to the platform API's ``/evals/report``.

Delivery semantics: an entry is XACKed only after the report POST attempt
completes (success, or terminally failed after bounded retries, logged). A worker
crash before that attempt leaves the entry pending, so the next redelivery re-runs
it -- an eval is never lost to a mid-run crash. A malformed payload cannot be
processed on any redelivery, so it is logged and acked (a poison-pill drop). A
missing/corrupt bundle or a provisioning failure is a failed run reported and
acked, not a crash; a failing eval case is a failed count in the report. A suite
that outruns its ADR-0131 delivery deadline is the same shape: abandoned,
reported as a failure, and acked -- a replacement inherits the SAME deadline, so
re-running would only spend a budget that is already gone.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from aci_protocol import (
    STREAM_PAYLOAD_FIELD,
    Budget,
    EvalJob,
    EvalReport,
    parse_eval_job,
)
from curie_telemetry import extract_trace_context, operation_span, record_metric
from opentelemetry.trace import SpanKind
from plugin_format import (
    DEFAULT_MAX_COMPRESSION_RATIO,
    DEFAULT_MAX_MEMBERS,
    DEFAULT_MAX_UNCOMPRESSED_BYTES,
    safe_extract,
)
from redis.asyncio import Redis

from ..binding import (
    BUDGET_ENV,
    BUNDLE_REF_ENV,
    BUNDLE_VERSION_ENV,
    PLUGIN_DIR_ENV,
    RUNNER_TOKEN_ENV,
    SESSION_ID_ENV,
    apply_model_env,
    inject_connector_secrets,
)
from ..bundle_store import BundleReader
from ..config import WorkerConfig
from ..consumer_liveness import ConsumerLivenessStore
from ..delivery_lease import DeliveryLeaseStore, LeaseLostError
from ..sandbox import SandboxSubstrate
from ..sandbox.types import SandboxError
from ..stream_consumer import DeliverySpec, ReadLoopSpec, StreamConsumer
from ..upgrade_drain import UpgradeDrainGate
from .models import EvalCaseResult, EvalOutcome, EvalRunResult, EvalScorer, EvalSuite
from .recorder import LangfuseEvalRecorder
from .run import run_eval_suite, sample_config_from_env
from .scorer import TrajectoryScorer
from .trajectory import TrajectorySidecar

logger = logging.getLogger(__name__)

# ``EvalJob``/``EvalReport`` are the shared wire models (#492), re-exported so
# this module stays the eval lane's seam for the stream payloads.
__all__ = [
    "EvalJob",
    "EvalReport",
    "EvalReporter",
    "EvalStreamConsumer",
    "load_suite_from_bundle",
]

# Pause before retrying the blocking eval read after a transient transport error.
_EVAL_READ_ERROR_BACKOFF_S = 0.5

# Page size for the PEL scan in the delivery-cap check (#535); mirrors the runs
# lane's _CAP_SCAN_PAGE so the whole pending list is cap-checked, not just its head.
_EVAL_CAP_SCAN_PAGE = 1000


@dataclass(frozen=True)
class _ExtractedEvalFiles:
    """Validated cases plus the optional run layer sidecar."""

    suite: EvalSuite
    trajectory_bytes: bytes | None


@dataclass(frozen=True)
class _LoadedEvalSuite:
    """One suite and the scorer selection resolved above the frozen port."""

    suite: EvalSuite
    scorer: TrajectoryScorer | None = None
    trajectory_error: str | None = None


class EvalReporter:
    """POSTs an EvalReport to the platform API's /evals/report, with retries."""

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        client: httpx.AsyncClient,
        max_attempts: int = 3,
        backoff_base_s: float = 0.5,
    ) -> None:
        self._url = f"{api_base_url.rstrip('/')}/evals/report"
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._client = client
        self._max_attempts = max_attempts
        self._backoff_base_s = backoff_base_s

    async def report(self, report: EvalReport) -> bool:
        """Attempt the report POST with bounded retries. True on success; False
        when terminally failed (logged) -- either way the caller may ack."""
        for attempt in range(1, self._max_attempts + 1):
            try:
                resp = await self._client.post(
                    self._url, json=report.model_dump(), headers=self._headers
                )
                resp.raise_for_status()
                return True
            except httpx.HTTPError as exc:
                if attempt >= self._max_attempts:
                    logger.error(
                        "eval report POST failed terminally for %s@%s: %s",
                        report.repo_full_name,
                        report.sha,
                        exc,
                    )
                    return False
                await asyncio.sleep(self._backoff_base_s * (2 ** (attempt - 1)))
        return False


def load_suite_from_bundle(
    data: bytes,
    suite_name: str,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
    max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
    max_members: int = DEFAULT_MAX_MEMBERS,
) -> EvalSuite | None:
    """Extract the bundle archive and load its evals/cases.json as an EvalSuite,
    named with ``suite_name`` (the payload's authoritative name / Langfuse tag).
    Returns None on a corrupt, unsafe, or oversized archive (ADR-0059
    decision 3), or a missing/invalid suite file. The size/ratio caps default
    to ``plugin_format``'s generous fallbacks; ``_load_suite`` passes the
    operator-configured ``WorkerConfig`` values instead."""
    files = _extract_eval_files(
        data,
        suite_name,
        max_uncompressed_bytes=max_uncompressed_bytes,
        max_compression_ratio=max_compression_ratio,
        max_members=max_members,
    )
    return None if files is None else files.suite


def _extract_eval_files(
    data: bytes,
    suite_name: str,
    *,
    max_uncompressed_bytes: int,
    max_compression_ratio: float,
    max_members: int,
) -> _ExtractedEvalFiles | None:
    """Extract the eval files used by the run layer."""

    try:
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            safe_extract(
                data,
                dest,
                max_uncompressed_bytes=max_uncompressed_bytes,
                max_compression_ratio=max_compression_ratio,
                max_members=max_members,
            )
            cases = next(
                (path for path in dest.rglob("cases.json") if path.parent.name == "evals"),
                None,
            )
            if cases is None:
                return None
            parsed = EvalSuite.model_validate_json(cases.read_bytes())
            trajectory = cases.parent / "trajectory.json"
            trajectory_bytes = trajectory.read_bytes() if trajectory.is_file() else None
    except Exception:
        logger.exception("could not load eval suite from bundle")
        return None

    return _ExtractedEvalFiles(
        suite=EvalSuite(name=suite_name, cases=parsed.cases),
        trajectory_bytes=trajectory_bytes,
    )


def _select_scorer(files: _ExtractedEvalFiles) -> _LoadedEvalSuite:
    """Validate an optional sidecar and select trajectory scoring when present."""

    if files.trajectory_bytes is None:
        return _LoadedEvalSuite(suite=files.suite)

    try:
        sidecar = TrajectorySidecar.model_validate_json(files.trajectory_bytes)
    except Exception as exc:
        return _LoadedEvalSuite(
            suite=files.suite,
            trajectory_error=f"invalid trajectory sidecar: {exc}",
        )

    case_ids = [case.id for case in files.suite.cases]
    if len(case_ids) != len(set(case_ids)):
        return _LoadedEvalSuite(
            suite=files.suite,
            trajectory_error="trajectory scoring rejects duplicate suite case ids",
        )

    return _LoadedEvalSuite(
        suite=files.suite,
        scorer=TrajectoryScorer(specs=sidecar.to_spec_map()),
    )


class EvalStreamConsumer(StreamConsumer):
    """Reads curie:evals, runs each suite, records, reports, then acks."""

    def __init__(
        self,
        *,
        redis: Redis,
        config: WorkerConfig,
        bundle_store: BundleReader,
        substrate: SandboxSubstrate,
        reporter: EvalReporter,
        recorder: LangfuseEvalRecorder,
        repo_lookup: Any,
        leases: DeliveryLeaseStore | None = None,
        drain: UpgradeDrainGate | None = None,
    ) -> None:
        # ADR-0131: "runs and evals must share the lease implementation by
        # construction. A fix on only one consumer lane is incomplete." The whole
        # lifecycle is the shared base's; this lane only hands it the store.
        #
        # It deliberately wires NO ``on_lease_lost``: an eval suite has no single
        # runner turn to interrupt through the kernel's bounded control path, so
        # there is nothing for the callback to stop. Active cancellation on lease
        # loss is the named follow-up (F2), and the residual is documented at
        # ``_report`` below -- not papered over with a second interrupt mechanism.
        # ``drain`` is the same pre-upgrade quiesce gate the runs lane takes
        # (#2010), and for the same construction reason ADR-0131 gives above: a
        # gate wired on only one lane is incomplete. An eval delivery holds a
        # sandbox and a lease exactly as a turn does.
        super().__init__(
            redis,
            leases=leases,
            drain=drain,
            liveness_store=ConsumerLivenessStore(redis),
        )
        self._config = config
        self._bundles = bundle_store
        self._substrate = substrate
        self._reporter = reporter
        self._recorder = recorder
        self._repo_lookup = repo_lookup
        # Bound concurrent in-flight SandboxClaim creation (#709). The read loop
        # and the reclaim loop both drive _handle -> _acquire_target under one
        # gather, so two claims can be created at once even though each loop is
        # itself sequential; unbounded that races a small cluster's binder and
        # produces the single-node 504/never-bind storm. This semaphore caps how
        # many claims are being created/bound at the same time (default 1: the
        # next claim waits until the current one binds -- sequential-with-
        # backpressure), so a suite completes on one node instead of flooding it.
        self._max_concurrent_claims = config.eval_max_concurrent_claims
        self._claim_slots = asyncio.Semaphore(config.eval_max_concurrent_claims)
        self._inflight: set[asyncio.Task[None]] = set()
        # The reclaim/dead-letter knobs the shared base machinery reads; handler
        # is the tracked/shielded self._dispatch. The eval-specific log strings stay
        # eval-specific (logger curie_worker.eval.stream) so eval dead-letters
        # remain alert-free -- they are NOT unified with the runs-lane strings.
        self._delivery = DeliverySpec(
            stream=config.eval_stream,
            group=config.eval_consumer_group,
            consumer=config.eval_consumer_name,
            dead_letter_target=config.eval_dead_letter_stream_name(),
            over_cap_reason="max-delivery-exceeded",
            max_delivery=config.max_delivery,
            dead_letter_maxlen=config.dead_letter_maxlen,
            reclaim_min_idle_ms=config.reclaim_min_idle_ms,
            # Eval can use the same prompt threshold as runs now that consumer
            # idle is only a candidate filter and the independent alive lease is
            # the liveness proof. Its lease refreshes through inline suite drain.
            dead_consumer_idle_ms=config.dead_consumer_idle_ms,
            heartbeat_ttl_ms=config.consumer_heartbeat_ttl_ms,
            capability_ttl_ms=config.consumer_capability_ttl_ms,
            read_count=config.read_count,
            cap_scan_page=_EVAL_CAP_SCAN_PAGE,
            telemetry_source="eval",
            handler=self._dispatch,
            logger=logger,
            dead_letter_log="dead-lettered eval entry %s after %d deliveries (reason=%s) -> %s",
            dead_letter_fail_log="dead-lettering eval entry %s failed; left pending, not re-run",
            # The same resolver the runs lane reads, because ADR-0131 requires both
            # consumer lanes to share the lease implementation by construction ("a
            # fix on only one consumer lane is incomplete"). Not a copy-paste: an
            # eval delivery whose owner was force-killed would otherwise wait out
            # the 900s backstop while a runs delivery recovered in one lease.
            lease_expired_idle_ms=config.lease_expired_idle_ms_value(),
        )

    async def ensure_group(self) -> None:
        # Create the group at a max-age cutoff rather than the stream head ("0").
        # Reading from "0" replays the ENTIRE stream history the first time the
        # group is created, so a stream that accumulated ancient entries (across
        # deploys, weeks of PRs) storms them all into the worker at once on first
        # boot. Starting at (now - eval_stream_max_age_hours) skips only long-dead
        # entries while still delivering recent backlog: an eval younger than the
        # window is never lost -- a short outage is covered -- but a week-old
        # requeue is not replayed. Crash recovery is unaffected: the reclaim loop
        # works off the pending list, not the group's start id. An existing group
        # keeps its position, so this only bounds the very first creation.
        cutoff_ms = int(
            (
                datetime.now(UTC) - timedelta(hours=self._config.eval_stream_max_age_hours)
            ).timestamp()
            * 1000
        )
        await self._ensure_group(
            self._config.eval_stream,
            self._config.eval_consumer_group,
            start_id=str(cutoff_ms),
        )

    async def run(self) -> None:
        await self.ensure_group()
        await self._run_consumer_generation(
            {
                "read": self._read_loop,
                "maintenance": self._reclaim_loop,
                "prompt-reclaim": self._prompt_reclaim_loop,
            }
        )

    def _generation_inflight_tasks(self) -> set[asyncio.Task[None]]:
        return set(self._inflight)

    def _reset_generation_resources(self) -> None:
        self._inflight.clear()
        self._claim_slots = asyncio.Semaphore(self._max_concurrent_claims)

    async def _read_loop(self) -> None:
        # count=1 is load-bearing, not a tunable: this consumer handles an entry
        # inline (evals are heavy and run sequentially), and only the entry inside
        # _handle is tracked as in-flight. Claiming a batch would leave the
        # un-handled tail claimed-but-untracked, where the reclaim loop could
        # re-run one before the read loop reaches it (a duplicate report). Peer
        # replicas in the group provide the parallelism instead.
        await self._consume(
            ReadLoopSpec(
                stream=self._config.eval_stream,
                group=self._config.eval_consumer_group,
                consumer=self._config.eval_consumer_name,
                count=1,
                block_ms=self._config.read_block_ms,
                backoff_s=_EVAL_READ_ERROR_BACKOFF_S,
                timeout_msg="eval stream read timed out (idle); retrying: %s",
                connection_msg="eval stream read failed transiently; retrying: %s",
                logger=logger,
            ),
            self._dispatch,
        )

    async def _reclaim_loop(self) -> None:
        """Periodically reclaim entries a dead consumer left pending and re-run
        them. Without this, an entry left pending by a crash before ``_ack``
        would sit in the group's PEL forever -- the at-least-once promise the
        ``_handle`` pending path relies on lives here."""
        while not self._should_stop():
            try:
                await self._reclaim_once()
            except Exception:
                logger.exception("eval reclaim tick failed")
            await self._sleep_or_stop(self._config.reclaim_interval_s)

    def _transfer_capacity(self) -> int | None:
        # The eval lane runs one suite at a time: ``_dispatch`` below awaits the
        # handler (``asyncio.shield``), so ``_dispatch_reclaimed`` runs suites
        # strictly one at a time. Subtract a suite already running inline so the
        # expiry pass claims nothing while one is in flight -- claiming a second
        # row would leave it XCLAIMed and unleased for the length of a whole eval
        # run.
        return max(0, 1 - len(self._inflight_ids))

    async def _dispatch(self, entry_id: str, fields: dict[str, str]) -> None:
        """Track the inline eval handler so graceful stop can drain it alive."""

        if entry_id in self._inflight_ids:
            return
        # Reserve before spawning so the read and reclaim producers cannot both
        # pass the duplicate check in the same event-loop turn.
        self._inflight_ids.add(entry_id)
        task = asyncio.create_task(self._handle(entry_id, fields))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)
        # The read/reclaim producer may be cancelled to stop new ownership, but
        # graceful shutdown keeps this handler alive and the lifecycle drains it
        # before deleting the consumer lease.
        await asyncio.shield(task)

    async def _handle(self, entry_id: str, fields: dict[str, str]) -> None:
        # Eval jobs are their own lifecycle. Ignore any adjacent turn carrier so
        # a reused transport field cannot fabricate causality across the sibling
        # stream; missing and malformed values therefore share this safe root.
        with operation_span(
            "curie.eval.process",
            kind=SpanKind.CONSUMER,
            parent=extract_trace_context({}),
            attributes={"service.name": "curie-worker", "source": "eval"},
        ):
            await self._handle_entry(entry_id, fields)

    async def _handle_entry(self, entry_id: str, fields: dict[str, str]) -> None:
        self._inflight_ids.add(entry_id)
        try:
            # ADR-0131: the same fence the runs lane holds, in the same shared
            # context manager. Placed inside the outer ``try`` and outside the
            # inner one so the ``finally`` below still discards the in-flight id
            # last, after the lease has been released.
            async with self._delivery_lease(entry_id, fields) as lease:
                if lease is None:
                    # Leased elsewhere: return WITHOUT acking so the entry stays
                    # pending for its true owner. A refusal is a normal early
                    # return, never an exception.
                    return
                try:
                    # The sanctioned tolerant decode: a newer API adding an optional
                    # field must not land in the poison-pill branch below, which
                    # would ack and DROP the job with no dead letter.
                    item = parse_eval_job(fields[STREAM_PAYLOAD_FIELD])
                except Exception:
                    # Poison pill: unprocessable on any redelivery. Dead-letter it
                    # (not a bare ack) so the malformed entry is observable in the
                    # eval graveyard, matching the runs lane's unparseable path (#535).
                    logger.exception(
                        "malformed eval work item %s; dead-lettering as poison", entry_id
                    )
                    record_metric(
                        "curie.eval.process",
                        attributes={
                            "service.name": "curie-worker",
                            "source": "eval",
                            "outcome": "failure",
                        },
                    )
                    await self._dead_letter(
                        entry_id, fields, reason="unparseable", delivery_count=1
                    )
                    return
                try:
                    result = await self._run_and_report(item, entry_id)
                    logger.info("eval %s @ %s: %s", item.suite, item.sha, result.summary())
                except Exception:
                    # An unexpected error before the report attempt: leave pending so
                    # the reclaim loop re-runs it (an eval must not be lost to a crash).
                    logger.exception("eval processing failed for %s; left pending", entry_id)
                    record_metric(
                        "curie.eval.process",
                        attributes={
                            "service.name": "curie-worker",
                            "source": "eval",
                            "outcome": "failure",
                        },
                    )
                    return
                metric_outcome = (
                    "plumbing"
                    if result.results
                    and all(row.outcome is EvalOutcome.PLUMBING_OK for row in result.results)
                    else (
                        "failure"
                        if any(row.outcome is EvalOutcome.FAIL for row in result.results)
                        else "success"
                    )
                )
                record_metric(
                    "curie.eval.process",
                    attributes={
                        "service.name": "curie-worker",
                        "source": "eval",
                        "outcome": metric_outcome,
                    },
                )
                try:
                    # A stale owner may not ACK: the entry belongs to whoever
                    # holds the fence now.
                    lease.raise_if_lost()
                except LeaseLostError:
                    logger.warning(
                        "refusing to ack eval entry %s: this owner lost the "
                        "delivery lease mid-suite; leaving it pending",
                        entry_id,
                    )
                    return
                await self._ack(entry_id)
                # Terminal acknowledgement removes the delivery STATE as well as
                # the lease; the base's release drops only the lease. Best-effort:
                # the ack above already happened.
                await self._settle_delivery_best_effort(entry_id)
        finally:
            self._inflight_ids.discard(entry_id)

    async def _run_and_report(self, item: EvalJob, stream_id: str) -> EvalRunResult:
        repo = await self._repo_lookup.repo_full_name(item.agent_id)
        loaded = await self._load_suite(item)
        if loaded is None:
            return await self._report_failed(
                item, repo, "unresolvable suite/bundle", stream_id=stream_id
            )

        suite = loaded.suite
        model = self._eval_model(item)
        if loaded.trajectory_error is not None:
            result = EvalRunResult(
                version=item.sha,
                suite=suite.name,
                model=model,
                stream_id=stream_id,
                scorer=EvalScorer.TRAJECTORY,
                results=[
                    EvalCaseResult(
                        case_id=case.id,
                        outcome=EvalOutcome.FAIL,
                        output="",
                        latency_ms=0.0,
                        detail=loaded.trajectory_error,
                    )
                    for case in suite.cases
                ],
            )
            if self._recorder is not None:
                await self._recorder.record(result)
            await self._report(item, repo, result, stream_id=stream_id)
            return result

        base_url, release_key, token = await self._acquire_target(item)
        if base_url is None:
            return await self._report_failed(
                item, repo, "runner provisioning failed", stream_id=stream_id
            )
        samples = sample_config_from_env(os.environ)
        # ADR-0131 gives a delivery ONE overall deadline, and it covers the whole
        # delivery -- not just the runs lane. Without this bound the eval lane
        # acquires a lease, lets the shared heartbeat renew it indefinitely, and
        # runs a suite that can outlive its own budget: the deadline would exist
        # in Valkey and be enforced nowhere. Read AFTER provisioning on purpose,
        # so the claim/bind wait is charged against the budget rather than gifted
        # back. ``asyncio.timeout`` is used instead of plumbing a deadline through
        # ``run_eval_suite``'s signature because cancellation genuinely STOPS the
        # in-flight case (a parameter would only be advisory), and widening that
        # frozen-ish run-layer API is a separate change.
        deadline_s = self._remaining_budget_s(stream_id)
        run_result: EvalRunResult | None = None
        try:
            async with asyncio.timeout(deadline_s):
                if loaded.scorer is None:
                    run_result = await run_eval_suite(
                        suite,
                        base_url=base_url,
                        version=item.sha,
                        recorder=self._recorder,
                        token=token,
                        model=model,
                        fake=self._config.fake_model,
                        samples=samples,
                        stream_id=stream_id,
                    )
                else:
                    run_result = await run_eval_suite(
                        suite,
                        base_url=base_url,
                        version=item.sha,
                        recorder=self._recorder,
                        token=token,
                        model=model,
                        fake=self._config.fake_model,
                        scorer=loaded.scorer,
                        samples=samples,
                        stream_id=stream_id,
                    )
        except TimeoutError:
            # Terminal for THIS delivery, not a crash to be reclaimed: the
            # deadline is inherited by a replacement (``HSETNX`` on
            # ``deadline_ms``), so re-running would only exhaust it again. Report
            # the failure -- through the fenced path below -- and let the caller
            # ack. Logged before the release so the deadline is named even if
            # teardown is slow.
            logger.error(
                "eval %s @ %s exceeded its delivery deadline (%.1fs of budget "
                "remained when the suite started); abandoning the suite",
                item.suite,
                item.sha,
                deadline_s if deadline_s is not None else -1.0,
            )
        finally:
            if release_key is not None:
                # wait_gone: Kubernetes delete returns before the pod is gone,
                # and a still-terminating eval sandbox pins ResourceQuota so the
                # ladder's post-eval cluster message cannot bind inside 45s
                # (#2259). The wait is here, after the suite and before the
                # matrix report the CLI polls, so eval returning means quota is
                # free.
                await asyncio.to_thread(self._substrate.release, release_key, wait_gone=True)

        if run_result is None:
            return await self._report_failed(
                item, repo, "delivery deadline exceeded", stream_id=stream_id
            )
        await self._report(item, repo, run_result, stream_id=stream_id)
        return run_result

    def _remaining_budget_s(self, stream_id: str) -> float | None:
        """Seconds left of this delivery's one deadline, or ``None`` when this
        lane is unfenced.

        Never negative: an already-spent budget must bound the run at zero rather
        than wrap into "no deadline", which is how a budget check turns into the
        opposite of itself. ``None`` (no recorded lease) maps to
        ``asyncio.timeout(None)`` -- unbounded, exactly the pre-ADR-0131
        behaviour a base-only consumer had. That is the UNFENCED lane: the shared
        base registers a lease in ``_held_leases`` only when there is a real one
        to register, never the permissive sentinel, so a consumer built with no
        lease store records nothing here and its suite is bounded by nothing --
        as it was before the ADR.
        """
        lease = self._held_leases.get(stream_id)
        if lease is None:
            return None
        return max(0.0, lease.remaining_s())

    async def _load_suite(self, item: EvalJob) -> _LoadedEvalSuite | None:
        if item.bundle_ref is None:
            return None
        try:
            data = await asyncio.to_thread(self._bundles.get, item.bundle_ref)
        except Exception:
            logger.exception("could not fetch bundle %s", item.bundle_ref)
            return None
        files = _extract_eval_files(
            data,
            item.suite,
            max_uncompressed_bytes=self._config.bundle_max_uncompressed_bytes,
            max_compression_ratio=self._config.bundle_max_compression_ratio,
            max_members=self._config.bundle_max_members,
        )
        return None if files is None else _select_scorer(files)

    async def _acquire_target(self, item: EvalJob) -> tuple[str | None, str | None, str | None]:
        if item.target_url is not None:
            # dev/test shortcut: eval a given runner. Not a claim of ours, so no
            # token -- the driver omits the header (only-when-configured).
            return item.target_url, None, None
        release_key = f"eval-{uuid.uuid4().hex}"
        try:
            connector_secrets = await self._repo_lookup.secrets_for(item.agent_id)
            thinking = await self._repo_lookup.thinking_for(item.agent_id)
            name_for = getattr(self._repo_lookup, "name_for", None)
            agent_name = await name_for(item.agent_id) if name_for is not None else None
            env = self._boot_env(item, connector_secrets, thinking)
            # Hold a claim slot only across creation/binding (the flood source),
            # not the whole suite run: the semaphore is released the moment the
            # claim binds, so the bound sandbox runs its cases while the next
            # claim may begin. The secrets/env prep above is cheap and does not
            # touch the cluster, so it stays outside the slot.
            async with self._claim_slots:
                handle = await asyncio.to_thread(
                    self._substrate.claim,
                    release_key,
                    env=env,
                    agent_name=agent_name,
                )
        except SandboxError:
            logger.exception("could not provision a runner for eval %s", item.sha)
            return None, None, None
        return handle.base_url, release_key, handle.token or None

    def _eval_model(self, item: EvalJob) -> str | None:
        """The model dimension for this run: the caller-requested ``item.model``
        when set (#526, a sweep pins each run to a distinct model), else the model
        the eval's runner is booted with (``config.model``, the same value
        ``apply_model_env`` forwards as ``CURIE_MODEL``). The dev/test
        ``target_url`` shortcut evals a runner we did not boot, so unless the caller
        named a model its model is unknown and left unlabelled.

        A fake-model install (the sealed default, or ``CURIE_FAKE_MODEL=1``) runs
        the canned ``FakeModelSession`` regardless of the requested model, so a row
        is left unlabelled rather than tagged with a model that was never called:
        labelling it would fabricate a cross-model comparison the sweep never
        performed (#606, ADR-0041's "an empty success is a lie"). The runner we did
        not boot (``target_url``) is exempt -- our fake flag says nothing about what
        it ran."""
        if item.target_url is not None:
            return item.model
        if self._config.fake_model:
            if item.model is not None:
                logger.warning(
                    "eval requested model %r but this install runs the fake model; "
                    "recording the row unlabelled rather than as a model never called",
                    item.model,
                )
            return None
        if item.model is not None:
            return item.model
        return self._config.model or None

    def _boot_env(
        self,
        item: EvalJob,
        connector_secrets: dict[str, str] | None = None,
        thinking: str | None = None,
    ) -> dict[str, str]:
        budget = Budget(
            max_output_tokens_per_run=self._config.default_max_output_tokens_per_run,
            max_usd_per_day=self._config.default_max_usd_per_day,
        )
        env = {
            BUDGET_ENV: budget.model_dump_json(),
            SESSION_ID_ENV: f"eval-{item.version_id}",
            PLUGIN_DIR_ENV: self._config.bundle_plugin_dir,
            RUNNER_TOKEN_ENV: secrets.token_urlsafe(32),
        }
        if item.bundle_ref is not None:
            env[BUNDLE_REF_ENV] = item.bundle_ref
        if item.sha:
            env[BUNDLE_VERSION_ENV] = item.sha
        # Deliver the agent's connector secrets (#429) so an authed-MCP bundle
        # authenticates during eval exactly as it does on a bound run -- otherwise
        # its tool calls fail auth and the eval measures the wrong thing. The
        # shared helper drops reserved boot-env names (#457) so a secret named after
        # a runner-owned env key or model credential can never clobber it, keeping
        # this write site hardened identically to binding.boot_env. The values are
        # resolved by the async caller (they need a DB lookup) and passed in.
        inject_connector_secrets(env, connector_secrets, agent_label=item.agent_id)
        # A caller-requested model (#526) wins over the worker default so the
        # provisioned sandbox actually runs the model this sweep row is measuring;
        # _eval_model tags the same value, keeping the boot and the matrix label
        # in lock-step. None falls back to config.model exactly as before.
        apply_model_env(
            env,
            self._config,
            model_override=item.model,
            thinking_override=thinking,
        )
        return env

    async def _report_failed(
        self, item: EvalJob, repo: str | None, reason: str, *, stream_id: str
    ) -> EvalRunResult:
        # ``stream_id`` is threaded in rather than carried on the result: the
        # failure DTO below has no run to attach one to, and the fence is
        # resolved from this argument (see ``_report``).
        logger.error("eval %s @ %s failed: %s", item.suite, item.sha, reason)
        result = EvalRunResult(version=item.sha, suite=item.suite, results=[])
        await self._report(item, repo, result, stream_id=stream_id)
        return result

    async def _report(
        self, item: EvalJob, repo: str | None, result: EvalRunResult, *, stream_id: str
    ) -> None:
        # ADR-0131, the eval lane's one user-visible terminal effect. A suite
        # whose fence has already moved must not publish a second report for a
        # job a replacement now owns, so the loss is checked immediately before
        # the send rather than only at the ack.
        #
        # The fence is resolved from the ``stream_id`` ARGUMENT, never from
        # ``result.stream_id``. The failure paths above build an
        # ``EvalRunResult`` with no run behind it, so that field is ``None`` on
        # exactly the "unresolvable bundle" / "provisioning failed" / "deadline
        # exceeded" cases -- the Valkey/K8s-adjacent failures most likely to
        # coincide with a lease loss. Looking the lease up by an absent field
        # would miss, and a missing lease read as "no fence" is a fence that
        # FAILS OPEN. Resolution is therefore explicit and total.
        #
        # This closes MOST of the window, not all of it: a lease lost between
        # this check and the platform's apply still produces a report that the
        # replacement will duplicate. Eval reports are therefore an explicitly
        # AT-LEAST-ONCE boundary -- the kind ADR-0131 requires be named rather
        # than papered over -- and closing it needs eval-report idempotency at
        # the platform API, tracked as follow-up F2.
        lease = self._held_leases.get(stream_id)
        if lease is None and self._leases is not None:
            # A fenced lane whose lease cannot be resolved for this entry: this
            # owner cannot prove it still holds authority, so it does not
            # publish. Fail closed, the same posture the lease store itself
            # takes. The ``self._leases is not None`` half is what keeps an
            # UNFENCED lane publishing: the base records no lease at all for it
            # (it registers only real ones, never the permissive sentinel), and
            # with no lease store there is no fence to fail closed on.
            logger.warning(
                "not reporting eval %s @ %s: no delivery lease is recorded for "
                "entry %s, so this owner cannot prove it still holds the fence",
                item.suite,
                item.sha,
                stream_id,
            )
            return
        if lease is not None and lease.lost.is_set():
            logger.warning(
                "not reporting eval %s @ %s: this owner lost the delivery lease "
                "mid-suite and a replacement owns the job",
                item.suite,
                item.sha,
            )
            return
        # The frozen EvalReport carries passed_count/total only, and the API turns
        # it into a GitHub commit status. That shape cannot express "ran but was
        # never graded": 0/N posts a red that did not happen and N/N posts the
        # false green this gate exists to prevent. So a run whose every case is
        # non-graded posts nothing -- no commit status is better than a fabricated
        # one. A run carrying any FAIL still reports: broken plumbing is a real red
        # and must reach the PR check. Skipping is a completed report attempt, so
        # the caller acks rather than leaving the entry pending forever.
        if result.all_plumbing():
            logger.info(
                "eval %s @ %s ran plumbing-only (%d cases, not graded); no report posted",
                item.suite,
                item.sha,
                result.total,
            )
            return
        await self._reporter.report(
            EvalReport(
                repo_full_name=repo or str(item.agent_id),
                sha=item.sha,
                passed_count=result.passed_count,
                total=result.total,
                target_url=item.target_url,
            )
        )
