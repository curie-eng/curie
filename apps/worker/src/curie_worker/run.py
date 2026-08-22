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
from typing import Any

import httpx
import redis
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .approval_cards import ApprovalCardStore
from .approvals import ApprovalClient
from .binding import BindingResolver
from .bundle_store import BundleStore
from .config import WorkerConfig
from .connector_loop import ConnectorReconcileLoop, HttpManifestSource
from .consumer import Consumer
from .dead_letter_alert import install_dead_letter_alerting
from .eval import EvalReporter, EvalStreamConsumer, LangfuseEvalRecorder
from .heartbeat import run_heartbeat
from .kernel import Kernel
from .killswitch import KillSwitch
from .markers import Markers
from .reply_sink import ReplySinkRouter, build_reply_sink
from .runner_client import RunnerClient
from .sandbox import (
    AffinityStore,
    DockerSandboxClient,
    KubernetesSandboxClient,
    RunnerHardening,
    SandboxClient,
    SandboxSubstrate,
    SubstrateConfig,
)
from .threadlock import ThreadLock

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
    # None unless the connector reconciler is enabled (ADR-0090, #1184). Held
    # here so `_run` supervises it beside the consumers rather than letting it
    # run unsupervised.
    connector_loop: ConnectorReconcileLoop | None = None


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
        if not env.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
            logger.warning(
                "Docker substrate selected but OTEL_EXPORTER_OTLP_ENDPOINT is "
                "unset; runner traces will not be exported"
            )
        client = DockerSandboxClient(
            image=env.get("CURIE_RUNNER_IMAGE", "curie-runner"),
            bundle_store=BundleStore(config),
            network=env.get("CURIE_DOCKER_NETWORK") or None,
            otel_endpoint=env.get("OTEL_EXPORTER_OTLP_ENDPOINT") or None,
            default_plugin_dir=config.bundle_plugin_dir,
            # Container isolation for every spawned runner (#631): read-only
            # rootfs, dropped caps, no-new-privileges, bounded resources. Mirrors
            # the K8s runner securityContext; overridable via CURIE_RUNNER_*.
            hardening=RunnerHardening.from_env(env),
            environ=env,
            bundle_max_uncompressed_bytes=config.bundle_max_uncompressed_bytes,
            bundle_max_compression_ratio=config.bundle_max_compression_ratio,
            bundle_max_members=config.bundle_max_members,
        )
        # Prewarm the runner image once at startup so the first claim window is
        # not gated on a cold pull. Best-effort inside ensure_image.
        client.ensure_image()
        return client
    return KubernetesSandboxClient(sub_config.namespace)


def build(config: WorkerConfig, env: Mapping[str, str]) -> Runtime:
    async_redis: AsyncRedis = AsyncRedis(
        host=config.valkey_host,
        port=config.valkey_port,
        password=config.valkey_password or None,
        db=config.valkey_db,
        decode_responses=True,
        socket_timeout=config.valkey_socket_timeout_s,
    )
    sync_redis = redis.Redis(
        host=config.valkey_host,
        port=config.valkey_port,
        password=config.valkey_password or None,
        db=config.valkey_db,
        decode_responses=True,
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
    )
    engine = create_async_engine(config.database_url, pool_pre_ping=True)
    binding = BindingResolver(engine, config)
    # One API-lane HTTP client shared by the approval writer (#244), the two
    # eval-lane reporters below, and (PROTOTYPE, Draft ADR-0115) the delegation
    # reply adapter; httpx.AsyncClient is task-safe.
    eval_http = httpx.AsyncClient(timeout=30.0)
    approval_client = ApprovalClient(
        api_base_url=config.api_base_url,
        api_key=config.api_key,
        client=eval_http,
        # This read runs inside per thread ordering. A short timeout safely
        # defers card settlement without changing creation or shared clients.
        read_timeout_s=2.0,
    )
    # PROTOTYPE (Draft ADR-0115): the delegation reply adapter shares this same
    # client, like the approval writer above.
    sink = build_reply_sink(config, http_client=eval_http)
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
        approvals=approval_client,
        # The same client, handed in twice under the two roles the kernel needs
        # (#1084). Two parameters rather than one so a test can fake the create
        # half without also implementing a read it never exercises.
        approval_reader=approval_client,
        card_store=ApprovalCardStore(async_redis, config),
    )
    killswitch = KillSwitch(async_redis, on_kill=kernel.interrupt_agent)
    kernel.attach_killswitch(killswitch)
    consumer = Consumer(redis=async_redis, kernel=kernel, config=config)

    # The eval lane (F3): a second consumer group on curie:evals, on its own
    # Valkey connection so its blocking read never stalls the runs consumer. It
    # reuses the same substrate (eval runs provision from the same warm pool) and
    # the binding resolver as its repo lookup for the /evals/report payload.
    eval_redis: AsyncRedis = AsyncRedis(
        host=config.valkey_host,
        port=config.valkey_port,
        password=config.valkey_password or None,
        db=config.valkey_db,
        decode_responses=True,
        socket_timeout=config.valkey_socket_timeout_s,
    )
    eval_consumer = EvalStreamConsumer(
        redis=eval_redis,
        config=config,
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
        connector_loop=_build_connector_loop(config, engine),
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
    logging.basicConfig(level=logging.INFO)
    install_dead_letter_alerting()
    resolved = env if env is not None else os.environ
    config = WorkerConfig()
    asyncio.run(_run(config, resolved))


if __name__ == "__main__":
    main()
