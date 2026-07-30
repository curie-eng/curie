"""FastAPI application factory and shared-resource lifespan.

The engine, sessionmaker, httpx client, and Langfuse client are created once at
startup and stored on app.state; dependencies (deps.py) read them per request.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from fastapi import FastAPI

from .config import get_settings
from .db import create_engine, create_sessionmaker
from .evalqueue import EvalQueue
from .github_checks import GitHubStatusReporter
from .graveyardwatcher import GraveyardWatcher
from .k8s import build_lazy_pod_lister, build_lazy_pod_log_reader
from .killswitch import KillSwitch
from .langfuse import LangfuseClient
from .resumequeue import ResumeQueue
from .resumereconciler import ResumeReconciler
from .routers import (
    agents,
    approvals,
    bundles,
    config,
    control,
    deployments,
    evals,
    github,
    memory,
    observability,
    runs,
    state,
)
from .slack_approvers import SlackApproverSetSelector
from .slack_usergroups import SlackUserGroupClient
from .storage import BundleStore
from .sweeper import run_expiry_sweeper
from .threadreset import ThreadResetRequests


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    engine = create_engine()
    app.state.engine = engine
    app.state.sessionmaker = create_sessionmaker(engine)
    http_client = httpx.AsyncClient(timeout=10.0)
    app.state.http_client = http_client
    app.state.langfuse = LangfuseClient(settings, http_client)
    store = BundleStore(settings)
    await store.ensure_bucket()
    app.state.bundle_store = store
    # Lazy: resolving the cluster/credentials is deferred to the first pod-log
    # read so an absent/expired credential does not surface as a boot-time ERROR
    # for a proxy most runs never touch.
    app.state.pod_log_reader = build_lazy_pod_log_reader(settings.kube_config_path)
    app.state.pod_lister = build_lazy_pod_lister(settings.kube_config_path)
    valkey: redis.Redis = redis.from_url(settings.valkey_dsn())
    app.state.valkey = valkey
    app.state.kill_switch = KillSwitch(valkey)
    app.state.thread_reset_requests = ThreadResetRequests(valkey)
    app.state.eval_queue = EvalQueue(valkey)
    # resume_dead_letter_stream stays the narrower override that wins when set;
    # its fallback is now the unified graveyard name (which honors
    # CURIE_DEAD_LETTER_STREAM / CURIE_STREAM via the shared derivation, #668)
    # instead of letting ResumeQueue re-derive `<runs_stream>:dead` from the base
    # stream alone -- so the backstop scans the SAME graveyard the worker writes.
    app.state.resume_queue = ResumeQueue(
        valkey,
        stream=settings.runs_stream,
        dead_letter_stream=settings.resume_dead_letter_stream or settings.dead_letter_stream_name(),
    )
    # The composition root for approvals (#420, ADR-0034): the only place that
    # names Slack to build the approver-set selector, so the authorizer and the
    # resolve endpoint depend on ports rather than on a provider. The usergroup
    # client shares the app's httpx client and is None when no bot token is
    # configured, which is the normal Slack-free deployment -- a route that
    # declares an approvers group then fails closed at resolve time rather than
    # silently widening.
    usergroups = (
        SlackUserGroupClient(
            http_client,
            token=settings.slack_bot_token,
            ttl_s=settings.slack_usergroup_cache_ttl_s,
        )
        if settings.slack_bot_token
        else None
    )
    app.state.approver_sets = SlackApproverSetSelector(usergroups)
    app.state.github_reporter = GitHubStatusReporter(
        http_client,
        api_url=settings.github_api_url,
        token=settings.github_token,
        context=settings.eval_check_context,
    )
    # The resume reconciler (#411) backstops a failed inline resume enqueue by
    # periodically re-enqueuing owed wakes. It enqueues via resume_queue (which
    # uses the valkey client), so it is cancelled BEFORE valkey.aclose() below.
    reconciler = ResumeReconciler(
        app.state.sessionmaker,
        app.state.resume_queue,
        interval_seconds=settings.resume_reconciler_interval_seconds,
        grace_seconds=settings.resume_reconciler_grace_seconds,
        batch_limit=settings.resume_reconciler_batch_limit,
        dead_letter_scan_limit=settings.resume_dead_letter_scan_limit,
    )
    app.state.resume_reconciler = reconciler
    app.state.resume_reconciler_task = (
        asyncio.create_task(reconciler.run_forever())
        if settings.resume_reconciler_enabled
        else None
    )
    # The expiry sweeper (#412) flips lapsed pending approvals and resumes their
    # stranded sessions. It shares this lifecycle's resources (sessionmaker,
    # resume_queue); interval <= 0 disables it (no task started).
    sweeper_stop: asyncio.Event | None = None
    if settings.approval_sweep_interval_s > 0:
        sweeper_stop = asyncio.Event()
        app.state.sweeper_task = asyncio.create_task(
            run_expiry_sweeper(
                app.state.sessionmaker,
                app.state.resume_queue,
                settings.approval_sweep_interval_s,
                sweeper_stop,
            )
        )
    else:
        app.state.sweeper_task = None
    # The dead-letter graveyard watcher (#531): read-only reader on
    # <runs_stream>:dead that alerts on each new dead-letter. Interval <= 0
    # disables it. Read-only, so it needs no ordering vs valkey.aclose() beyond
    # being cancelled before it.
    if settings.dead_letter_watch_interval_s > 0:
        watcher = GraveyardWatcher(
            valkey,
            stream=settings.dead_letter_stream_name(),
            interval_seconds=settings.dead_letter_watch_interval_s,
        )
        app.state.graveyard_watcher = watcher
        app.state.graveyard_watcher_task = asyncio.create_task(watcher.run_forever())
    else:
        app.state.graveyard_watcher_task = None
    try:
        yield
    finally:
        # Both background loops enqueue via resume_queue (which uses the valkey
        # client) and read via the sessionmaker, so both are stopped BEFORE
        # valkey.aclose()/engine.dispose() below.
        task = getattr(app.state, "resume_reconciler_task", None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # Stop the sweeper BEFORE closing valkey/engine so an in-flight pass does
        # not race the closed clients. The wait-first loop wakes immediately on
        # stop.set(); wait_for already cancels on timeout, so suppressing
        # TimeoutError/CancelledError is the whole teardown.
        if sweeper_stop is not None:
            sweeper_stop.set()
            try:
                await asyncio.wait_for(app.state.sweeper_task, 5.0)
            except (TimeoutError, asyncio.CancelledError):
                pass
        # Read-only, so simply cancel it before closing valkey.
        watcher_task = getattr(app.state, "graveyard_watcher_task", None)
        if watcher_task is not None:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass
        await valkey.aclose()
        await http_client.aclose()
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Curie API", version="0.1.0", lifespan=lifespan)

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(config.router)
    app.include_router(agents.router)
    app.include_router(deployments.router)
    app.include_router(bundles.router)
    app.include_router(github.router)
    app.include_router(observability.router)
    app.include_router(control.router)
    app.include_router(evals.router)
    app.include_router(runs.router)
    app.include_router(state.router)
    app.include_router(memory.router)
    app.include_router(approvals.router)
    return app


app = create_app()
