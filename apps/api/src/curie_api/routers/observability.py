"""Observability API (OB1): Langfuse-backed metrics + runner-pod log proxy."""

from fastapi import APIRouter, Depends, HTTPException, status
from starlette.concurrency import run_in_threadpool

from .. import metrics as metrics_service
from ..auth import require_api_key
from ..config import get_settings
from ..deps import LangfuseDep, PodListerDep, PodLogReaderDep
from ..k8s import NoClusterConfigured, PodLogError
from ..schemas import MetricSeries, MetricsSummary, PodLogs, RunnerPods

router = APIRouter(
    prefix="/observability",
    tags=["observability"],
    dependencies=[Depends(require_api_key)],
)


def _resolve_window(start: str | None, end: str | None) -> tuple[str, str]:
    window = get_settings().metrics_default_window_hours
    try:
        return metrics_service.resolve_window(start, end, window)
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"start/end must be ISO 8601 timestamps: {exc}",
        ) from exc


@router.get("/metrics/summary", response_model=MetricsSummary)
async def metrics_summary(
    lf: LangfuseDep,
    start: str | None = None,
    end: str | None = None,
    environment: str | None = None,
    agent: str | None = None,
) -> MetricsSummary:
    start_iso, end_iso = _resolve_window(start, end)
    return await metrics_service.summary(lf, start_iso, end_iso, environment, agent)


@router.get("/metrics/series", response_model=MetricSeries)
async def metrics_series(
    lf: LangfuseDep,
    metric: str,
    start: str | None = None,
    end: str | None = None,
    granularity: str = "day",
    environment: str | None = None,
    agent: str | None = None,
) -> MetricSeries:
    if metric not in metrics_service.ALL_METRICS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"unknown metric {metric!r}; choose one of {metrics_service.ALL_METRICS}",
        )
    start_iso, end_iso = _resolve_window(start, end)
    return await metrics_service.series(
        lf, metric, start_iso, end_iso, granularity, environment, agent
    )


@router.get("/runners", response_model=RunnerPods)
async def list_runner_pods(
    lister: PodListerDep,
    namespace: str | None = None,
) -> RunnerPods:
    settings = get_settings()
    ns = namespace or settings.runner_namespace
    try:
        pods = await run_in_threadpool(
            lister.list_runner_pods, ns, settings.runner_pod_label_selector
        )
    except NoClusterConfigured as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except PodLogError as exc:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(exc)) from exc
    return RunnerPods(namespace=ns, pods=pods)


@router.get("/runners/{namespace}/{pod}/logs", response_model=PodLogs)
async def runner_logs(
    namespace: str,
    pod: str,
    reader: PodLogReaderDep,
    lister: PodListerDep,
    container: str | None = None,
    tail_lines: int | None = None,
    previous: bool = False,
) -> PodLogs:
    settings = get_settings()
    if namespace != settings.runner_namespace:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"logs are only available for the runner namespace {settings.runner_namespace!r}",
        )
    try:
        runner_pods = await run_in_threadpool(
            lister.list_runner_pods,
            settings.runner_namespace,
            settings.runner_pod_label_selector,
        )
    except NoClusterConfigured as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except PodLogError as exc:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(exc)) from exc
    if pod not in runner_pods:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"{pod!r} is not a runner pod in namespace {settings.runner_namespace!r}",
        )
    try:
        logs = await run_in_threadpool(reader.read, namespace, pod, container, tail_lines, previous)
    except NoClusterConfigured as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except PodLogError as exc:
        code = status.HTTP_404_NOT_FOUND if exc.status == 404 else status.HTTP_502_BAD_GATEWAY
        raise HTTPException(code, str(exc)) from exc
    return PodLogs(namespace=namespace, pod=pod, container=container, logs=logs)
