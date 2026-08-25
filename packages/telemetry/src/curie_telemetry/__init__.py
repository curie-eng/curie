"""Shared internal telemetry contract for Curie services."""

from .bootstrap import (
    ServiceTelemetry,
    bootstrap_service_telemetry,
    build_otlp_span_exporter,
)
from .config import resolve_otlp_endpoint, resolve_otlp_protocol
from .context import (
    TRACEPARENT_STREAM_FIELD,
    extract_trace_context,
    inject_trace_context,
)
from .logging import configure_service_logging
from .metrics import configure_meter_provider, record_metric
from .resource import build_resource, deployment_environment, service_instance_id
from .tracing import operation_span

__all__ = [
    "TRACEPARENT_STREAM_FIELD",
    "ServiceTelemetry",
    "bootstrap_service_telemetry",
    "build_otlp_span_exporter",
    "build_resource",
    "configure_meter_provider",
    "configure_service_logging",
    "deployment_environment",
    "extract_trace_context",
    "inject_trace_context",
    "operation_span",
    "record_metric",
    "resolve_otlp_endpoint",
    "resolve_otlp_protocol",
    "service_instance_id",
]
