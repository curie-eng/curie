"""FastAPI application factory and shared-resource lifespan.

The engine, sessionmaker, httpx client, and Langfuse client are created once at
startup and stored on app.state; dependencies (deps.py) read them per request.
"""

import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from curie_telemetry import (
    TRACEPARENT_STREAM_FIELD,
    bootstrap_service_telemetry,
    canonicalize_traceparent,
    configure_service_logging,
    extract_trace_context,
    operation_span,
    record_metric,
)
from fastapi import FastAPI, Request
from opentelemetry.trace import SpanKind, StatusCode
from starlette.routing import Match

from . import __version__
from .commitpoller import CommitPoller, GitHubBranchTip
from .config import get_settings
from .db import create_engine, create_sessionmaker
from .evalqueue import EvalQueue
from .github_app import credentials_for, log_credential_path
from .github_checks import GitHubStatusReporter
from .github_review_store import GitHubReviewReconciler
from .graveyardwatcher import GraveyardWatcher
from .k8s import build_lazy_pod_lister, build_lazy_pod_log_reader
from .killswitch import KillSwitch
from .langfuse import LangfuseClient
from .resumequeue import ResumeQueue
from .resumereconciler import ResumeReconciler
from .routers import (
    actions,
    agents,
    approvals,
    bundles,
    channels,
    cluster_message_replies,
    config,
    console,
    control,
    deploy_targets,
    deployments,
    evals,
    gitflow_routing,
    github,
    github_reviews,
    hooks,
    memory,
    observability,
    publications,
    runs,
    state,
    workspaces,
)
from .schema_compat import assert_servable
from .slack_approvers import SlackApproverSetSelector
from .slack_usergroups import SlackUserGroupClient
from .storage import BundleStore
from .sweeper import run_expiry_sweeper
from .threadreset import ThreadResetRequests

_LOG = logging.getLogger("curie_api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    # Fail closed when this image cannot serve the live schema. Migrations are
    # applied by the upgrade Job / curie-migrate, never here (#2300).
    await assert_servable()
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
    app.state.github_review_reconciler = GitHubReviewReconciler(
        app.state.sessionmaker,
        valkey,
        settings,
    )
    app.state.github_review_reconciler_task = (
        asyncio.create_task(app.state.github_review_reconciler.run_forever())
        if settings.github_review_reconciler_interval_s > 0
        else None
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
                publication_patch_retention_seconds=(
                    settings.publication_patch_retention_seconds
                ),
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
    # Commit polling (#1239): the deploy path for a cluster that cannot receive
    # a GitHub webhook. Interval <= 0 disables it, which is the default -- an
    # install whose webhook works needs nothing here, and polling would only
    # add GitHub API calls. It reuses the credential resolver and the ordinary
    # process_push, so it cannot disagree with the webhook about what a push
    # means.
    if settings.commit_poll_interval_s > 0:
        poller = CommitPoller(
            session_factory=app.state.sessionmaker,
            store=app.state.bundle_store,
            settings=settings,
            eval_queue=app.state.eval_queue,
            tips=GitHubBranchTip(settings, credentials_for(settings)),
            interval_seconds=settings.commit_poll_interval_s,
        )
        app.state.commit_poller = poller
        app.state.commit_poller_task = asyncio.create_task(poller.run_forever())
    else:
        app.state.commit_poller_task = None
    telemetry = bootstrap_service_telemetry(
        "curie-api",
        service_version=__version__,
        logger=logging.getLogger("curie_api"),
        environ=os.environ,
        level=settings.log_level.upper(),
    )
    app.state.telemetry = telemetry
    try:
        yield
    finally:
        review_task = app.state.github_review_reconciler_task
        if review_task is not None:
            review_task.cancel()
            try:
                await review_task
            except asyncio.CancelledError:
                pass
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
        # Cancelled before engine.dispose(): a poll pass mid-deploy holds a
        # session, and disposing the engine underneath it would raise on the
        # way out rather than shutting down cleanly.
        poller_task = getattr(app.state, "commit_poller_task", None)
        if poller_task is not None:
            poller_task.cancel()
            try:
                await poller_task
            except asyncio.CancelledError:
                pass
        try:
            await valkey.aclose()
            await http_client.aclose()
            await engine.dispose()
        finally:
            telemetry.shutdown()


def configure_logging(level: str | None = None) -> logging.Logger:
    """Give this service's loggers a level and a handler (#1270).

    `uvicorn curie_api.main:app` configures only the `uvicorn*` loggers and no
    root entry, so `curie_api.*` has an effective level of WARNING and no
    handler -- every INFO record in the service is dropped by the last-resort
    handler. That is production, not a local artifact: the Dockerfile runs bare
    uvicorn.

    Scoped to the `curie_api` logger rather than root, and `propagate` is off:
    configuring root would fight uvicorn's own dictConfig and anything the OTel
    wiring later attaches, and leaving propagation on would double-emit every
    record through whatever root ends up with.

    Idempotent. Tests construct many apps in one process, and a handler added
    per construction would multiply every line.
    """

    resolved = (level or get_settings().log_level).upper()
    return configure_service_logging(
        logging.getLogger("curie_api"),
        service_name="curie-api",
        level=resolved,
    )


def _route_template(app: FastAPI, request: Request) -> str:
    """Resolve a bounded route template without using identifier-bearing paths."""

    partial: str | None = None
    for route in app.routes:
        match, _child_scope = route.matches(request.scope)
        if match is Match.FULL:
            path = getattr(route, "path", None)
            if path:
                return str(path)
            included = getattr(route, "original_router", None)
            for candidate in getattr(included, "routes", ()):
                candidate_match, _ = candidate.matches(request.scope)
                candidate_path = getattr(candidate, "path", None)
                if candidate_match is Match.FULL and candidate_path:
                    return str(candidate_path)
                if candidate_match is Match.PARTIAL and candidate_path:
                    partial = str(candidate_path)
        elif match is Match.PARTIAL:
            path = getattr(route, "path", None)
            if path:
                partial = str(path)
    return partial or "unmatched"


_HTTP_METHOD_DOMAIN = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"})


def _metric_http_method(method: str) -> str:
    """Map arbitrary HTTP tokens onto the metric schema's finite domain."""

    normalized = method.upper()
    return normalized if normalized in _HTTP_METHOD_DOMAIN else "OTHER"


def create_app() -> FastAPI:
    configure_logging()
    # Which credential the platform will clone with (ADR-0092, #1262). One
    # line, no secret, and a warning when the App is set up only halfway.
    log_credential_path(get_settings())
    app = FastAPI(title="Curie API", version="0.1.0", lifespan=lifespan)

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(config.router)
    app.include_router(console.router)
    app.include_router(agents.router)
    app.include_router(deployments.router)
    app.include_router(bundles.router)
    app.include_router(deploy_targets.router)
    app.include_router(github.router)
    app.include_router(github_reviews.router)
    app.include_router(gitflow_routing.router)
    app.include_router(observability.router)
    app.include_router(control.router)
    app.include_router(evals.router)
    app.include_router(runs.router)
    app.include_router(state.router)
    app.include_router(memory.router)
    app.include_router(approvals.router)
    app.include_router(actions.router)
    app.include_router(publications.router)
    app.include_router(publications.internal_router)
    app.include_router(cluster_message_replies.router)
    app.include_router(cluster_message_replies.internal_router)
    app.include_router(workspaces.router)
    app.include_router(channels.router)
    app.include_router(hooks.router)

    @app.middleware("http")
    async def observe_http(request: Request, call_next):  # type: ignore[no-untyped-def]
        operation = _route_template(app, request)
        inbound_traceparent = request.headers.get(TRACEPARENT_STREAM_FIELD)
        canonical_traceparent = canonicalize_traceparent(inbound_traceparent)
        parent = extract_trace_context(
            {TRACEPARENT_STREAM_FIELD: canonical_traceparent}
            if canonical_traceparent is not None
            else {}
        )
        diagnostic = (
            "trace.context.missing"
            if inbound_traceparent is None
            else "trace.context.malformed"
            if canonical_traceparent is None
            else None
        )
        active_attributes = {
            "service.name": "curie-api",
            "operation": operation,
            "role": "server",
            "source": _metric_http_method(request.method),
        }
        started = time.monotonic()
        status_code = 500
        record_metric("curie.http.server.active", 1, attributes=active_attributes)
        try:
            with operation_span(
                "http.server.request",
                kind=SpanKind.SERVER,
                parent=parent,
                attributes=active_attributes,
            ) as span:
                if diagnostic is not None:
                    span.add_event(diagnostic)
                response = await call_next(request)
                status_code = response.status_code
                if status_code >= 500 and hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                _LOG.info(
                    "http request completed method=%s route=%s status_class=%s",
                    _metric_http_method(request.method),
                    operation,
                    f"{status_code // 100}xx",
                )
                return response
        finally:
            outcome = f"{status_code // 100}xx" if 100 <= status_code < 600 else "5xx"
            completed_attributes = {**active_attributes, "outcome": outcome}
            record_metric("curie.http.server.request", attributes=completed_attributes)
            record_metric(
                "curie.http.server.request.duration",
                time.monotonic() - started,
                attributes=completed_attributes,
            )
            record_metric("curie.http.server.active", -1, attributes=active_attributes)

    return app


app = create_app()
