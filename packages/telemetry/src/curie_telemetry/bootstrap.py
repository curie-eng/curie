"""Bounded standard OTLP setup for Curie platform services."""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import unquote, urlsplit, urlunsplit

from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter as GrpcOTLPLogExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter as GrpcOTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter as GrpcOTLPSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http._log_exporter import (
    OTLPLogExporter as HttpOTLPLogExporter,
)
from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
    OTLPMetricExporter as HttpOTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter as HttpOTLPSpanExporter,
)
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .config import resolve_otlp_endpoint, resolve_otlp_protocol
from .logging import configure_service_logging
from .metrics import configure_meter_provider
from .resource import build_resource, deployment_environment, service_instance_id
from .tracing import configure_tracer_provider

_QUEUE_SIZE = 2048
_BATCH_SIZE = 512
_EXPORT_TIMEOUT_MILLIS = 5000
_SCHEDULE_DELAY_MILLIS = 1000


def _exporter_endpoint(
    signal: str,
    env: Mapping[str, str],
    *,
    protocol: str,
    honor_sdk_disabled: bool = True,
) -> str | None:
    endpoint = resolve_otlp_endpoint(
        signal,
        env,
        honor_sdk_disabled=honor_sdk_disabled,
    )
    if endpoint is None:
        return None
    signal_key = f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT"
    if signal_key in env:
        return endpoint
    if protocol == "grpc":
        return endpoint
    parsed = urlsplit(endpoint)
    signal_path = f"{parsed.path.rstrip('/')}/v1/{signal}"
    return urlunsplit(parsed._replace(path=signal_path))


def _exporter_headers(signal: str, env: Mapping[str, str]) -> dict[str, str] | None:
    signal_key = f"OTEL_EXPORTER_OTLP_{signal.upper()}_HEADERS"
    raw = env[signal_key] if signal_key in env else env.get("OTEL_EXPORTER_OTLP_HEADERS", "")
    # The SDK helper includes the rejected header value in its warning. That
    # value is commonly an Authorization credential and this path runs before
    # service logging is configured, so parse locally and keep diagnostics
    # value-free.
    parsed: dict[str, str] = {}
    malformed = False
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, separator, value = item.partition("=")
        if not separator or not name.strip() or not value.strip():
            malformed = True
            continue
        parsed[unquote(name).strip().lower()] = unquote(value).strip()
    if malformed:
        logging.getLogger(__name__).warning(
            "ignored malformed OTLP exporter header entry"
        )
    return parsed or None


def build_otlp_span_exporter(
    environ: Mapping[str, str], *, honor_sdk_disabled: bool = True
) -> GrpcOTLPSpanExporter | HttpOTLPSpanExporter | None:
    """Construct the trace exporter selected by standard protocol precedence."""

    if (
        resolve_otlp_endpoint(
            "traces",
            environ,
            honor_sdk_disabled=honor_sdk_disabled,
        )
        is None
    ):
        return None
    protocol = resolve_otlp_protocol("traces", environ)
    endpoint = _exporter_endpoint(
        "traces",
        environ,
        protocol=protocol,
        honor_sdk_disabled=honor_sdk_disabled,
    )
    headers = _exporter_headers("traces", environ)
    if endpoint is None:
        return None
    if protocol == "grpc":
        return GrpcOTLPSpanExporter(
            endpoint=endpoint,
            headers=tuple(headers.items()) if headers is not None else None,
        )
    return HttpOTLPSpanExporter(endpoint=endpoint, headers=headers)


def _log_exporter(
    env: Mapping[str, str],
) -> GrpcOTLPLogExporter | HttpOTLPLogExporter | None:
    if resolve_otlp_endpoint("logs", env, honor_sdk_disabled=True) is None:
        return None
    protocol = resolve_otlp_protocol("logs", env)
    endpoint = _exporter_endpoint("logs", env, protocol=protocol)
    headers = _exporter_headers("logs", env)
    if endpoint is None:
        return None
    if protocol == "grpc":
        return GrpcOTLPLogExporter(
            endpoint=endpoint,
            headers=tuple(headers.items()) if headers is not None else None,
        )
    return HttpOTLPLogExporter(endpoint=endpoint, headers=headers)


def _metric_exporter(
    env: Mapping[str, str],
) -> GrpcOTLPMetricExporter | HttpOTLPMetricExporter | None:
    if resolve_otlp_endpoint("metrics", env, honor_sdk_disabled=True) is None:
        return None
    protocol = resolve_otlp_protocol("metrics", env)
    endpoint = _exporter_endpoint("metrics", env, protocol=protocol)
    headers = _exporter_headers("metrics", env)
    if endpoint is None:
        return None
    if protocol == "grpc":
        return GrpcOTLPMetricExporter(
            endpoint=endpoint,
            headers=tuple(headers.items()) if headers is not None else None,
        )
    return HttpOTLPMetricExporter(endpoint=endpoint, headers=headers)


def _tracer_provider(resource: Resource, env: Mapping[str, str]) -> TracerProvider | None:
    exporter = build_otlp_span_exporter(env)
    if exporter is None:
        return None
    provider = TracerProvider(resource=resource, shutdown_on_exit=False)
    provider.add_span_processor(
        BatchSpanProcessor(
            exporter,
            max_queue_size=_QUEUE_SIZE,
            schedule_delay_millis=_SCHEDULE_DELAY_MILLIS,
            max_export_batch_size=_BATCH_SIZE,
            export_timeout_millis=_EXPORT_TIMEOUT_MILLIS,
        )
    )
    return provider


def _logger_provider(resource: Resource, env: Mapping[str, str]) -> LoggerProvider | None:
    exporter = _log_exporter(env)
    if exporter is None:
        return None
    provider = LoggerProvider(resource=resource, shutdown_on_exit=False)
    provider.add_log_record_processor(
        BatchLogRecordProcessor(
            exporter,
            max_queue_size=_QUEUE_SIZE,
            schedule_delay_millis=_SCHEDULE_DELAY_MILLIS,
            max_export_batch_size=_BATCH_SIZE,
            export_timeout_millis=_EXPORT_TIMEOUT_MILLIS,
        )
    )
    return provider


def _meter_provider(resource: Resource, env: Mapping[str, str]) -> MeterProvider:
    exporter = _metric_exporter(env)
    readers = (
        [
            PeriodicExportingMetricReader(
                exporter,
                export_interval_millis=10000,
                export_timeout_millis=_EXPORT_TIMEOUT_MILLIS,
            )
        ]
        if exporter is not None
        else []
    )
    return MeterProvider(metric_readers=readers, resource=resource, shutdown_on_exit=False)


def _bounded_call(target: object, method: str, timeout_millis: int) -> bool:
    complete = threading.Event()

    def invoke() -> None:
        try:
            function = getattr(target, method)
            if method == "force_flush":
                function(timeout_millis=timeout_millis)
            else:
                function()
        except BaseException:
            pass
        finally:
            complete.set()

    thread = threading.Thread(target=invoke, daemon=True)
    thread.start()
    return complete.wait(timeout_millis / 1000)


@dataclass
class ServiceTelemetry:
    tracer_provider: TracerProvider | None
    logger_provider: LoggerProvider | None
    meter_provider: MeterProvider
    _closed: bool = field(default=False, init=False)

    def shutdown(self, *, timeout_millis: int = _EXPORT_TIMEOUT_MILLIS) -> None:
        if self._closed:
            return
        self._closed = True
        providers = [
            provider
            for provider in (
                self.tracer_provider,
                self.logger_provider,
                self.meter_provider,
            )
            if provider is not None
        ]
        for provider in providers:
            _bounded_call(provider, "force_flush", timeout_millis)
        for provider in providers:
            _bounded_call(provider, "shutdown", timeout_millis)


def bootstrap_service_telemetry(
    service_name: str,
    *,
    service_version: str,
    logger: logging.Logger,
    environ: Mapping[str, str] | None = None,
    level: int | str = logging.INFO,
) -> ServiceTelemetry:
    """Initialize traces, logs, and metrics without a localhost fallback."""

    env = os.environ if environ is None else environ
    resource = build_resource(
        service_name,
        service_version=service_version,
        service_instance_id=service_instance_id(service_name),
        deployment_environment=deployment_environment(env),
    )
    tracer_provider = _tracer_provider(resource, env)
    logger_provider = _logger_provider(resource, env)
    meter_provider = _meter_provider(resource, env)
    configure_tracer_provider(tracer_provider)
    configure_meter_provider(meter_provider)
    configure_service_logging(
        logger,
        service_name=service_name,
        logger_provider=logger_provider,
        level=level,
    )
    return ServiceTelemetry(tracer_provider, logger_provider, meter_provider)
