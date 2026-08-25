"""Exporter construction follows standard per-signal protocol precedence."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

import pytest
from curie_telemetry import bootstrap as bootstrap_module


class _ExporterProbe:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


@pytest.mark.parametrize(
    ("signal", "builder", "grpc_name", "http_name"),
    (
        (
            "traces",
            bootstrap_module.build_otlp_span_exporter,
            "GrpcOTLPSpanExporter",
            "HttpOTLPSpanExporter",
        ),
        (
            "logs",
            bootstrap_module._log_exporter,
            "GrpcOTLPLogExporter",
            "HttpOTLPLogExporter",
        ),
        (
            "metrics",
            bootstrap_module._metric_exporter,
            "GrpcOTLPMetricExporter",
            "HttpOTLPMetricExporter",
        ),
    ),
)
@pytest.mark.parametrize(
    ("general_protocol", "signal_protocol", "expected_protocol"),
    (
        ("http/protobuf", "grpc", "grpc"),
        ("grpc", "http/protobuf", "http/protobuf"),
    ),
)
def test_signal_protocol_selects_exporter_and_endpoint_shape(
    signal: str,
    builder: Callable[[Mapping[str, str]], object],
    grpc_name: str,
    http_name: str,
    general_protocol: str,
    signal_protocol: str,
    expected_protocol: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    grpc_instances: list[_ExporterProbe] = []
    http_instances: list[_ExporterProbe] = []

    def grpc_exporter(**kwargs: Any) -> _ExporterProbe:
        instance = _ExporterProbe(**kwargs)
        grpc_instances.append(instance)
        return instance

    def http_exporter(**kwargs: Any) -> _ExporterProbe:
        instance = _ExporterProbe(**kwargs)
        http_instances.append(instance)
        return instance

    monkeypatch.setattr(bootstrap_module, grpc_name, grpc_exporter)
    monkeypatch.setattr(bootstrap_module, http_name, http_exporter)
    environment = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.example.com:4317",
        "OTEL_EXPORTER_OTLP_PROTOCOL": general_protocol,
        f"OTEL_EXPORTER_OTLP_{signal.upper()}_PROTOCOL": signal_protocol,
        f"OTEL_EXPORTER_OTLP_{signal.upper()}_HEADERS": "authorization=placeholder",
    }

    exporter = builder(environment)

    selected = grpc_instances if expected_protocol == "grpc" else http_instances
    rejected = http_instances if expected_protocol == "grpc" else grpc_instances
    assert exporter is selected[0]
    assert rejected == []
    expected_endpoint = (
        "http://otel-collector.example.com:4317"
        if expected_protocol == "grpc"
        else f"http://otel-collector.example.com:4317/v1/{signal}"
    )
    assert selected[0].kwargs["endpoint"] == expected_endpoint
    headers = selected[0].kwargs["headers"]
    assert dict(headers) == {"authorization": "placeholder"}


@pytest.mark.parametrize(
    ("signal", "builder"),
    (
        ("traces", bootstrap_module.build_otlp_span_exporter),
        ("logs", bootstrap_module._log_exporter),
        ("metrics", bootstrap_module._metric_exporter),
    ),
)
def test_explicit_empty_signal_endpoint_keeps_export_disabled(
    signal: str,
    builder: Callable[[Mapping[str, str]], object],
) -> None:
    assert (
        builder(
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.example.com:4318",
                f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT": "",
                f"OTEL_EXPORTER_OTLP_{signal.upper()}_PROTOCOL": "grpc",
            }
        )
        is None
    )


@pytest.mark.parametrize(
    "builder",
    (
        bootstrap_module.build_otlp_span_exporter,
        bootstrap_module._log_exporter,
        bootstrap_module._metric_exporter,
    ),
)
def test_no_endpoint_remains_disabled_even_with_invalid_protocol(
    builder: Callable[[Mapping[str, str]], object],
) -> None:
    assert builder({"OTEL_EXPORTER_OTLP_PROTOCOL": "unsupported"}) is None


def test_malformed_header_diagnostic_never_echoes_credential(
    caplog: pytest.LogCaptureFixture,
) -> None:
    credential = "Bearer TOP-SECRET-OTLP-PLACEHOLDER"
    caplog.set_level(logging.WARNING)

    headers = bootstrap_module._exporter_headers(
        "logs",
        {"OTEL_EXPORTER_OTLP_LOGS_HEADERS": f"malformed {credential}"},
    )

    assert headers is None
    assert "ignored malformed OTLP exporter header entry" in caplog.text
    assert credential not in caplog.text
