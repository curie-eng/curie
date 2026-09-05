"""Process entrypoint: wire the kernel and consumer, then run.

Reads the environment, builds the async Valkey client (stream, locks, markers),
a sync Valkey client for the substrate's affinity store, the sandbox substrate,
the runner HTTP client, and the Slack sink, then runs the consumer until a
signal asks it to stop. Run with ``python -m curie_worker``.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import signal
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import redis
from aci_protocol.s3 import build_s3_client
from curie_telemetry import bootstrap_service_telemetry
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from . import __version__
from .actions import ActionClient
from .approval_cards import ApprovalCardStore
from .approvals import ApprovalClient
from .binding import BindingResolver
from .bundle_store import BundleStore
from .config import WorkerConfig
from .connector_loop import ConnectorReconcileLoop, HttpManifestSource
from .consumer import Consumer
from .dead_letter_alert import install_dead_letter_alerting
from .delivery_lease import DeliveryLeaseStore
from .eval import EvalReporter, EvalStreamConsumer, LangfuseEvalRecorder
from .heartbeat import run_heartbeat
from .kernel import Kernel
from .killswitch import KillSwitch
from .markers import Markers
from .publication_clients import (
    GitHubPublicationLookup,
    PublicationCredentialClient,
    PublicationTranscriptClient,
)
from .publication_k8s import KubernetesPublicationCluster, PublicationJobSettings
from .publication_loop import (
    PublicationReconcileLoop,
    PublicationReconciler,
)
from .publication_store import PostgresPublicationStore
from .reply_sink import ObservedReplySink, ReplySinkRouter, build_reply_sink
from .runner_client import RunnerClient
from .sandbox import (
    AffinityStore,
    DockerSandboxClient,
    KubernetesSandboxClient,
    RunnerHardening,
    SandboxClient,
    SandboxSubstrate,
    SubstrateConfig,
    SuspendedThreadError,
)
from .threadlock import ThreadLock
from .upgrade_drain import UpgradeDrainGate
from .workspace import (
    SubprocessCommands,
    WorkspaceClaimCoordinator,
    WorkspaceCredentialClient,
    WorkspaceLimits,
    WorkspaceObjectStore,
    WorkspacePreparer,
)

logger = logging.getLogger(__name__)


@dataclass
class Runtime:
    """The wired worker: the two Valkey consumers (runs + evals) plus the
    resources whose lifetimes they share, so ``_run`` can drive and dispose them."""

    consumer: Consumer
    killswitch: KillSwitch
    eval_consumer: EvalStreamConsumer
    runner: RunnerClient
    # Held here for the same reason the runner client is: the HTTP egress adapter
    # keeps ONE aiohttp session for the process's life, so the sink needs a
    # disposal site alongside the other long-lived transports.
    sink: ReplySinkRouter
    async_redis: AsyncRedis
    eval_redis: AsyncRedis
    eval_http: httpx.AsyncClient
    engine: AsyncEngine
    # The SAME store the kernel settles cards through, exposed so ``_run`` can
    # drive its one-shot legacy rekey at boot (#1751) without constructing a
    # second client-and-config pair that could drift from the kernel's.
    card_store: ApprovalCardStore
    # None unless the connector reconciler is enabled (ADR-0090, #1184). Held
    # here so `_run` supervises it beside the consumers rather than letting it
    # run unsupervised.
    connector_loop: ConnectorReconcileLoop | None = None
    publication_loop: PublicationReconcileLoop | None = None


# 365 days, the ceiling shared by all three operator-tunable seconds knobs
# (#1388). It is the smallest bound unambiguously above every legitimate
# setting: 365x the shipped suspended_route_ttl_seconds default (86400) and
# 8760x the route_ttl_seconds default (3600). It also sits ~9 orders of
# magnitude below the boundary where Valkey's millisecond expiry arithmetic
# overflows a signed 64-bit value and the store answers "value is not an
# integer or out of range", so that failure class becomes structurally
# unreachable rather than merely unlikely. And it doubles as an accumulation
# guard in the spirit of #1380: route expiry IS the orphan signal reap_orphans
# keys off, so a year-long route already means "never reaped by TTL" and
# anything longer is definitionally a leak.
_MAX_TUNABLE_SECONDS = 31_536_000


# How long the one-shot legacy approval-card rekey (#1751) may hold up boot.
# Not an operator knob: the number that matters is not "how big is the keyspace"
# but "how long may readiness stall", and that answer is the same everywhere.
# WHY a bound exists at all: this pass is awaited BEFORE the asyncio.gather that
# starts the liveness heartbeat, so nothing is touching the heartbeat file while
# it runs. Per-entry errors are swallowed and the loop continues, which means a
# degraded Valkey costs roughly one socket timeout per entry with nothing
# capping the total -- and the k8s exec probe, finding a stale heartbeat, kills
# the pod. Unbounded, a best-effort migration becomes a restart loop that never
# reaches the consumers. Cutting it short only leaves some legacy refs behind,
# and those lapse with their own TTL.
_CARD_MIGRATION_BUDGET_S = 30.0


def _bounded_seconds(
    env: Mapping[str, str], name: str, cast: Callable[[str], int | float]
) -> int | float | None:
    """Read one operator-tunable seconds knob, or None when it is unset.

    None means "leave the SubstrateConfig default in place", so an install that
    sets nothing is unaffected. Everything else is refused at boot with a
    ValueError naming the env var and the offending value, because these numbers
    reach Valkey as expiries: a non-positive TTL makes ``SET ... EX 0`` raise a
    ResponseError the kernel does not classify, and ``EXPIRE key 0`` silently
    DELETES the route instead of erroring. Refusing here turns a first-message
    hang that leaks a sandbox per attempt into a startup failure an operator can
    read (#1388).
    """
    raw = env.get(name)
    if raw is None:
        return None
    if not raw.strip():
        raise ValueError(
            f"{name} is set to an empty value ({raw!r}): unset it to take the "
            f"default, or set a number of seconds greater than 0 and at most "
            f"{_MAX_TUNABLE_SECONDS}"
        )
    try:
        value = cast(raw)
    except (TypeError, ValueError) as exc:
        kind = "a whole number of seconds" if cast is int else "a number of seconds"
        raise ValueError(f"{name} must be {kind}, got {raw!r}") from exc
    # inf (from "inf" or an overflowing literal like "1e400") makes the claim
    # deadline never elapse, so the wait spins forever inside the per-thread
    # lock; nan compares False against everything, so the wait never runs and
    # every claim fails instantly. The bounds check below does reject all three
    # on its own, but only because ``0 < nan`` happens to evaluate False --
    # implicit IEEE semantics a later "simplification" of that line would break
    # silently. This branch states the intent and owns the clearer message.
    # It is scoped to the float path because only a float can be non-finite,
    # and math.isfinite() on an int too large to convert to float raises
    # OverflowError, which would escape this helper as something other than a
    # ValueError naming the knob. Such an int is finite; the bounds check
    # rejects it, in integer arithmetic, as out of range.
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number of seconds, got {raw!r}")
    if not 0 < value <= _MAX_TUNABLE_SECONDS:
        raise ValueError(
            f"{name} must be greater than 0 and at most {_MAX_TUNABLE_SECONDS} "
            f"seconds (365 days), got {raw!r}"
        )
    return value


def _substrate_config(env: Mapping[str, str]) -> SubstrateConfig:
    # claim_timeout is overridable so a slow cluster can raise it; when unset the
    # authoritative default lives in SubstrateConfig. Keep any override below the
    # per-thread lock TTL (WorkerConfig.lock_ttl_ms) -- see that comment.
    overrides: dict[str, Any] = {}
    claim_timeout = _bounded_seconds(env, "CURIE_CLAIM_TIMEOUT_SECONDS", float)
    if claim_timeout is not None:
        overrides["claim_timeout_seconds"] = claim_timeout
    # The route TTLs govern how long a thread PINS its sandbox, which is the
    # term that decides how many exist at once. Leaving them hardcoded while
    # claim_timeout was tunable gave operators the deadline knob but not the
    # accumulation knob -- and raising a deadline only makes a doomed turn fail
    # slower (#1380). Both are exposed because they are the same mechanism:
    # capping the live TTL while a suspended route still pins a sandbox for a
    # day would just move the accumulation.
    route_ttl = _bounded_seconds(env, "CURIE_ROUTE_TTL_SECONDS", int)
    if route_ttl is not None:
        overrides["route_ttl_seconds"] = route_ttl
    suspended_route_ttl = _bounded_seconds(env, "CURIE_SUSPENDED_ROUTE_TTL_SECONDS", int)
    if suspended_route_ttl is not None:
        overrides["suspended_route_ttl_seconds"] = suspended_route_ttl
    return SubstrateConfig(
        namespace=env.get("CURIE_NAMESPACE", "default"),
        warm_pool=env.get("CURIE_WARM_POOL", "curie-runner-pool"),
        runner_port=int(env.get("CURIE_RUNNER_PORT", "8080")),
        **overrides,
    )


# The SDK credential env the runner authenticates a real model with; presence of
# either satisfies the local-middle-mode credential requirement.
_MODEL_CREDENTIAL_ENV = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY")


def _workspace_limits(config: WorkerConfig) -> WorkspaceLimits:
    """One typed envelope shared by preparation and Docker extraction."""

    return WorkspaceLimits(
        clone_timeout_seconds=config.workspace_clone_timeout_seconds,
        archive_timeout_seconds=config.workspace_archive_timeout_seconds,
        upload_timeout_seconds=config.workspace_upload_timeout_seconds,
        total_timeout_seconds=config.workspace_total_timeout_seconds,
        max_checkout_bytes=config.workspace_max_checkout_bytes,
        max_archive_bytes=config.workspace_max_archive_bytes,
        max_members=config.workspace_max_members,
        max_compression_ratio=config.workspace_max_compression_ratio,
        reference_ttl_seconds=config.workspace_reference_ttl_seconds,
        max_concurrent_clones=config.workspace_max_concurrent_clones,
    )


def _sandbox_client(
    config: WorkerConfig, env: Mapping[str, str], sub_config: SubstrateConfig
) -> SandboxClient:
    """The cluster/Docker seam, chosen by ``CURIE_SANDBOX_SUBSTRATE``.

    ``kubernetes`` (default) claims agent-sandbox CRs; ``docker`` boots runner
    containers locally (middle mode on a laptop, no cluster). The eval consumer
    shares the substrate this client backs, so the choice applies to both lanes.

    Local middle mode defaults to a REAL model. Fake model is an explicit
    offline/test opt-in, so a Docker worker with neither a model credential,
    ``CURIE_MODEL_BASE_URL``, nor ``CURIE_FAKE_MODEL`` fails loudly here
    rather than booting a real runner that would fail cryptically or silently
    degrading to a fake. A credential can be an SDK var
    (``CLAUDE_CODE_OAUTH_TOKEN`` / ``ANTHROPIC_API_KEY``) or the ACI
    ``CURIE_CREDENTIALS`` reference, which the runner maps onto an SDK var.
    """
    substrate = env.get("CURIE_SANDBOX_SUBSTRATE", "kubernetes").lower()
    if substrate == "docker":
        bundle_store = BundleStore(config)
        has_credential = bool(config.credentials) or any(v in env for v in _MODEL_CREDENTIAL_ENV)
        has_local_model = bool(config.model_base_url)
        if not config.fake_model and not has_credential and not has_local_model:
            raise SystemExit(
                "Local middle mode (CURIE_SANDBOX_SUBSTRATE=docker) defaults to a "
                "real model, but no model credential is set. Export "
                "CURIE_CREDENTIALS, CLAUDE_CODE_OAUTH_TOKEN, or ANTHROPIC_API_KEY "
                "before starting the worker, set CURIE_MODEL_BASE_URL to a local "
                "endpoint for local-model mode, or set CURIE_FAKE_MODEL=1 for an "
                "offline/test run."
            )
        runner_otel_endpoint = env.get(
            "CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT",
            env.get("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
        )
        if not runner_otel_endpoint:
            logger.warning(
                "Docker substrate selected but the runner OTLP endpoint is unset; "
                "runner traces will not be exported"
            )
        client = DockerSandboxClient(
            image=env.get("CURIE_RUNNER_IMAGE", "curie-runner"),
            bundle_store=bundle_store,
            network=env.get("CURIE_DOCKER_NETWORK") or None,
            otel_endpoint=runner_otel_endpoint or None,
            default_plugin_dir=config.bundle_plugin_dir,
            # Container isolation for every spawned runner (#631): read-only
            # rootfs, dropped caps, no-new-privileges, bounded resources. Mirrors
            # the K8s runner securityContext; overridable via CURIE_RUNNER_*.
            hardening=RunnerHardening.from_env(env),
            environ=env,
            bundle_max_uncompressed_bytes=config.bundle_max_uncompressed_bytes,
            bundle_max_compression_ratio=config.bundle_max_compression_ratio,
            bundle_max_members=config.bundle_max_members,
            workspace_limits=_workspace_limits(config),
        )
        # Prewarm the runner image once at startup so the first claim window is
        # not gated on a cold pull. Best-effort inside ensure_image.
        client.ensure_image()
        return client
    return KubernetesSandboxClient(sub_config.namespace)


def build(config: WorkerConfig, env: Mapping[str, str]) -> Runtime:
    async_redis: AsyncRedis = AsyncRedis(
        **config.valkey_client_kwargs(),
        socket_timeout=config.valkey_socket_timeout_s,
    )
    sync_redis = redis.Redis(
        **config.valkey_client_kwargs(),
        socket_timeout=config.valkey_socket_timeout_s,
    )
    sub_config = _substrate_config(env)
    substrate = SandboxSubstrate(
        _sandbox_client(config, env, sub_config),
        AffinityStore(sync_redis),
        sub_config,
    )
    runner = RunnerClient(
        connect_timeout_s=config.runner_connect_timeout_s,
        total_timeout_s=config.runner_total_timeout_s,
        snapshot_patch_max_bytes=config.publication_patch_max_bytes,
    )
    engine = create_async_engine(config.database_url, pool_pre_ping=True)
    binding = BindingResolver(engine, config)
    workspace_objects = WorkspaceObjectStore(
        client=build_s3_client(
            endpoint_url=config.s3_endpoint_url,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            region=config.s3_region,
        ),
        bucket=config.workspace_bucket,
        prefix=config.workspace_object_prefix,
    )
    workspace = (
        WorkspaceClaimCoordinator(
            preparer=WorkspacePreparer(
                credentials=WorkspaceCredentialClient(
                    api_url=config.api_base_url,
                    worker_token=config.internal_worker_token,
                ),
                commands=SubprocessCommands(),
                objects=workspace_objects,
                scratch_root=Path(config.workspace_scratch_root),
                limits=_workspace_limits(config),
            ),
            substrate=substrate,
            suspended_error=SuspendedThreadError,
            ownership_ttl_seconds=sub_config.route_ttl_seconds,
        )
        if config.workspace_enabled
        else None
    )
    # One API-lane HTTP client shared by the approval writer (#244) and the two
    # eval-lane reporters below; httpx.AsyncClient is task-safe.
    eval_http = httpx.AsyncClient(timeout=30.0)
    approval_client = ApprovalClient(
        api_base_url=config.api_base_url,
        api_key=config.api_key,
        client=eval_http,
        worker_token=config.internal_worker_token,
        # This read runs inside per thread ordering. A short timeout safely
        # defers card settlement without changing creation or shared clients.
        read_timeout_s=2.0,
    )
    action_client = ActionClient(
        api_base_url=config.api_base_url,
        api_key=config.api_key,
        client=eval_http,
    )
    sink = build_reply_sink(config)
    card_store = ApprovalCardStore(async_redis, config)
    kernel = Kernel(
        substrate=substrate,
        runner=runner,
        sink=sink,
        lock=ThreadLock(
            async_redis,
            ttl_ms=config.lock_ttl_ms,
            acquire_timeout_s=config.lock_acquire_timeout_s,
            poll_interval_s=config.lock_poll_interval_s,
        ),
        markers=Markers(async_redis, config),
        config=config,
        binding=binding,
        workspace=workspace,
        approvals=approval_client,
        # Publication is cluster-only in v1. A local request sees an actionable
        # refusal in the kernel before either durable row is created.
        publication_creator=(
            approval_client
            if config.publication_enabled
            and env.get("CURIE_SANDBOX_SUBSTRATE", "kubernetes").lower()
            == "kubernetes"
            else None
        ),
        # The same client, handed in twice under the two roles the kernel needs
        # (#1084). Two parameters rather than one so a test can fake the create
        # half without also implementing a read it never exercises.
        approval_reader=approval_client,
        actions=action_client,
        card_store=card_store,
        route_ttl_seconds=sub_config.route_ttl_seconds,
        suspended_route_ttl_seconds=sub_config.suspended_route_ttl_seconds,
    )
    killswitch = KillSwitch(async_redis, on_kill=kernel.interrupt_agent)
    kernel.attach_killswitch(killswitch)
    # Delivery ownership leases (ADR-0131), built from the CONCRETE async client
    # for the same reason ``Markers`` above is: the fence needs Lua scripting and
    # server ``TIME``, which the ``StreamBroker`` port deliberately does not
    # carry. Each lane gets its own store bound to its own connection, so the
    # eval lane's blocking read can never stall a runs-lane heartbeat.

    # The pre-upgrade drain gate (#2010). ONE gate object shared by both
    # delivery lanes: the quiesce flag is release-wide, and two gates reading
    # the same key would only be two ways to answer the same question.
    drain_gate = UpgradeDrainGate(async_redis, config)
    consumer = Consumer(
        redis=async_redis,
        kernel=kernel,
        config=config,
        leases=DeliveryLeaseStore(async_redis, config),
        drain=drain_gate,
    )

    # The eval lane (F3): a second consumer group on curie:evals, on its own
    # Valkey connection so its blocking read never stalls the runs consumer. It
    # reuses the same substrate (eval runs provision from the same warm pool) and
    # the binding resolver as its repo lookup for the /evals/report payload.
    eval_redis: AsyncRedis = AsyncRedis(
        **config.valkey_client_kwargs(),
        socket_timeout=config.valkey_socket_timeout_s,
    )
    eval_consumer = EvalStreamConsumer(
        redis=eval_redis,
        config=config,
        leases=DeliveryLeaseStore(eval_redis, config),
        drain=drain_gate,
        bundle_store=BundleStore(config),
        substrate=substrate,
        reporter=EvalReporter(
            api_base_url=config.api_base_url,
            api_key=config.api_key,
            client=eval_http,
            max_attempts=config.report_max_attempts,
            backoff_base_s=config.report_backoff_base_s,
        ),
        recorder=LangfuseEvalRecorder(
            base_url=config.langfuse_host,
            public_key=config.langfuse_public_key,
            secret_key=config.langfuse_secret_key,
            client=eval_http,
        ),
        repo_lookup=binding,
    )
    publication_loop = _build_publication_loop(
        config,
        env,
        engine,
        sink,
        eval_http,
        card_store,
    )
    return Runtime(
        consumer=consumer,
        killswitch=killswitch,
        eval_consumer=eval_consumer,
        runner=runner,
        sink=sink,
        async_redis=async_redis,
        eval_redis=eval_redis,
        eval_http=eval_http,
        engine=engine,
        card_store=card_store,
        connector_loop=_build_connector_loop(config, engine),
        publication_loop=publication_loop,
    )


async def _supervise(
    name: str,
    factory: Callable[[], Awaitable[None]],
    shutdown: asyncio.Event,
    *,
    restart_backoff_s: float = 1.0,
) -> None:
    """Run a worker task, restarting it if it crashes, until shutdown is requested.

    Each consumer's ``run()`` returns only when its own stop is requested. If one
    instead raises -- a latent bug, or an error that escaped its own read loop --
    restarting it keeps its siblings (runs, evals, killswitch, heartbeat) alive
    rather than letting the exception propagate out of the top-level gather and
    tear the whole worker down (#673). Paired with ``return_exceptions=True`` on
    that gather, this is the defence-in-depth behind the per-entry isolation in
    ``StreamConsumer._consume``. ``CancelledError`` is a ``BaseException`` and
    still propagates, so cooperative shutdown is unaffected.

    ``factory`` is a thunk (e.g. a bound ``run`` method) so each restart gets a
    fresh coroutine; ``run()`` is re-entrant (group creation is BUSYGROUP-safe).
    """
    while not shutdown.is_set():
        try:
            await factory()
            return
        except Exception:
            if shutdown.is_set():
                return
            logger.exception("worker task %s crashed; restarting", name)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=restart_backoff_s)
            except TimeoutError:
                pass


def _build_connector_loop(
    config: WorkerConfig, engine: AsyncEngine
) -> ConnectorReconcileLoop | None:
    """The connector reconcile loop, or None when it is switched off.

    Constructed here rather than inside the loop so a bad kubeconfig fails at
    boot, next to the flag that asked for it, instead of once per interval in a
    background task nobody is watching.
    """

    if not config.connector_reconcile_enabled:
        return None
    from .connector_k8s import KubernetesConnectorClient

    return ConnectorReconcileLoop(
        engine=engine,
        source=HttpManifestSource(
            api_base_url=config.api_base_url,
            api_key=config.api_key,
            release=config.connector_release,
            namespace=config.connector_namespace,
            app_name=config.connector_app_name,
        ),
        client=KubernetesConnectorClient(),
        namespace=config.connector_namespace,
        db_schema=config.db_schema,
        interval_seconds=config.connector_reconcile_interval_s,
    )


def _build_publication_loop(
    config: WorkerConfig,
    env: Mapping[str, str],
    engine: AsyncEngine,
    sink: ReplySinkRouter,
    http: httpx.AsyncClient,
    card_store: ApprovalCardStore,
) -> PublicationReconcileLoop | None:
    """Build the worker-owned Kubernetes publication lane, never a local twin."""

    if not config.publication_enabled or env.get(
        "CURIE_SANDBOX_SUBSTRATE", "kubernetes"
    ).lower() != "kubernetes":
        return None
    namespace = config.publication_namespace
    cluster = KubernetesPublicationCluster(namespace)
    store = PostgresPublicationStore(
        engine,
        schema=config.db_schema,
        lease_owner=config.consumer_name,
        lease_seconds=config.publication_lease_seconds,
        result_max_attempts=config.publication_result_max_attempts,
        reconcile_max_attempts=config.publication_reconcile_max_attempts,
    )
    reconciler = PublicationReconciler(
        store=store,
        credentials=PublicationCredentialClient(
            api_base_url=config.api_base_url,
            worker_token=config.internal_worker_token,
            client=http,
        ),
        cluster=cluster,
        github=GitHubPublicationLookup(http),
        # Publication delivery runs outside the kernel, so it must decorate the
        # shared router independently. The reply observation depth suppresses
        # the HTTP adapter's nested instrumentation and keeps one logical
        # delivery at exactly one span/metric pair.
        replies=ObservedReplySink(sink),
        card_store=card_store,
        transcript=(
            PublicationTranscriptClient(
                api_base_url=config.api_base_url,
                api_key=config.api_key,
                client=http,
            )
            if config.api_key
            else None
        ),
        job_settings=PublicationJobSettings(
            namespace=namespace,
            runner_image=env.get("CURIE_RUNNER_IMAGE", "curie-runner"),
            image_pull_policy=config.publication_image_pull_policy,
            image_pull_secrets=config.publication_image_pull_secrets,
            priority_class_name=config.publication_priority_class_name,
            service_account_name=config.publication_service_account_name,
            owner_name=config.publication_owner_name,
            git_user_name=config.publication_git_user_name,
            git_user_email=config.publication_git_user_email,
            github_api_url=config.publication_github_api_url,
            active_deadline_seconds=(
                config.publication_job_active_deadline_seconds
            ),
            git_timeout_seconds=config.publication_git_command_timeout_seconds,
            cpu_request=config.publication_cpu_request,
            cpu_limit=config.publication_cpu_limit,
            memory_request=config.publication_memory_request,
            memory_limit=config.publication_memory_limit,
            ephemeral_request=config.publication_ephemeral_request,
            ephemeral_limit=config.publication_ephemeral_limit,
        ),
    )
    return PublicationReconcileLoop(
        store=store,
        reconciler=reconciler,
        interval_seconds=config.publication_reconcile_interval_seconds,
    )


async def _run(config: WorkerConfig, env: Mapping[str, str]) -> None:
    rt = build(config, env)

    loop = asyncio.get_running_loop()

    # A single shutdown flag governs every supervised task. The liveness
    # heartbeat runs on this same event loop, so a wedged loop stops touching the
    # file and the k8s exec probe restarts the pod (issue #71).
    shutdown = asyncio.Event()

    def _stop() -> None:
        rt.consumer.request_stop()
        rt.killswitch.request_stop()
        rt.eval_consumer.request_stop()
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    logging.getLogger("curie_worker").info("worker starting")
    # One-shot, before any consumer reads: rekey approval-card refs left under
    # the pre-#1723 thread key onto their approval id, so an approval that was
    # already pending when this worker rolled can still have its card settled
    # (#1751). It is deliberately NOT supervised or repeated -- it is a boot
    # migration that no-ops once the old key space is empty -- and it is
    # deliberately swallowed: a Valkey blip here must degrade to "those cards
    # stay live until TTL", never to a worker that will not start.
    # It is bounded because the heartbeat has not started yet -- see
    # _CARD_MIGRATION_BUDGET_S.
    try:
        await asyncio.wait_for(
            rt.card_store.migrate_legacy_thread_keyed_refs(),
            timeout=_CARD_MIGRATION_BUDGET_S,
        )
    except TimeoutError:
        logger.warning(
            "legacy approval card migration exceeded its %.0fs boot budget and was "
            "cut short; any legacy ref it did not reach simply lapses with its TTL",
            _CARD_MIGRATION_BUDGET_S,
        )
    except Exception:
        logger.exception("legacy approval card migration failed; continuing boot")
    try:
        # return_exceptions=True + per-task restart: a crash in one consumer must
        # not cancel its siblings (#673). Supervisors only return on shutdown.
        await asyncio.gather(
            _supervise("runs", rt.consumer.run, shutdown),
            _supervise("killswitch", rt.killswitch.run, shutdown),
            _supervise("evals", rt.eval_consumer.run, shutdown),
            _supervise(
                "heartbeat",
                lambda: run_heartbeat(config.heartbeat_file, config.heartbeat_interval_s, shutdown),
                shutdown,
            ),
            *(
                [
                    _supervise(
                        "connectors",
                        lambda: rt.connector_loop.run_forever(shutdown),  # type: ignore[union-attr]
                        shutdown,
                    )
                ]
                if rt.connector_loop is not None
                else []
            ),
            *(
                [
                    _supervise(
                        "publications",
                        lambda: rt.publication_loop.run_forever(shutdown),  # type: ignore[union-attr]
                        shutdown,
                    )
                ]
                    if getattr(rt, "publication_loop", None) is not None
                else []
            ),
            return_exceptions=True,
        )
    finally:
        await rt.runner.close()
        await rt.sink.aclose()
        await rt.eval_http.aclose()
        await rt.async_redis.aclose()
        await rt.eval_redis.aclose()
        await rt.engine.dispose()
    logging.getLogger("curie_worker").info("worker stopped")


def main(env: Mapping[str, str] | None = None) -> None:
    resolved = env if env is not None else os.environ
    telemetry = bootstrap_service_telemetry(
        "curie-worker",
        service_version=__version__,
        logger=logging.getLogger("curie_worker"),
        environ=resolved,
    )
    try:
        install_dead_letter_alerting()
        config = WorkerConfig()
        asyncio.run(_run(config, resolved))
    finally:
        telemetry.shutdown()


if __name__ == "__main__":
    main()
