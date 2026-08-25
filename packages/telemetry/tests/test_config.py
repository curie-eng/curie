"""Standard OTLP environment resolution for platform service bootstraps."""

from __future__ import annotations

import pytest
from curie_telemetry import resolve_otlp_endpoint, resolve_otlp_protocol
from curie_telemetry.bootstrap import _exporter_endpoint


@pytest.mark.parametrize("signal", ("traces", "logs", "metrics"))
@pytest.mark.parametrize(
    "environ",
    (
        {},
        {"OTEL_EXPORTER_OTLP_ENDPOINT": ""},
        {"OTEL_EXPORTER_OTLP_ENDPOINT": "   "},
    ),
)
def test_unset_or_empty_endpoint_disables_remote_export(
    signal: str, environ: dict[str, str]
) -> None:
    assert resolve_otlp_endpoint(signal, environ, honor_sdk_disabled=True) is None


@pytest.mark.parametrize("signal", ("traces", "logs", "metrics"))
def test_signal_endpoint_uses_standard_override_precedence(signal: str) -> None:
    base = "http://otel-collector.example.com:4318"
    signal_endpoint = f"http://otel-collector.example.com:4318/v1/{signal}"
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": base,
        f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT": signal_endpoint,
    }

    assert resolve_otlp_endpoint(signal, environ, honor_sdk_disabled=True) == signal_endpoint


@pytest.mark.parametrize("signal", ("traces", "logs", "metrics"))
@pytest.mark.parametrize(
    ("base", "expected_prefix"),
    (
        (
            "https://otel-collector.example.com:4318?tenant=example#primary",
            "https://otel-collector.example.com:4318",
        ),
        (
            "https://otel-collector.example.com:4318/ingest/root?tenant=example#primary",
            "https://otel-collector.example.com:4318/ingest/root",
        ),
        (
            "https://otel-collector.example.com:4318/ingest/root/?tenant=example#primary",
            "https://otel-collector.example.com:4318/ingest/root",
        ),
    ),
)
def test_generic_http_endpoint_appends_signal_path_before_query_and_fragment(
    signal: str, base: str, expected_prefix: str
) -> None:
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": base,
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
    }

    assert _exporter_endpoint(
        signal,
        environ,
        protocol=resolve_otlp_protocol(signal, environ),
    ) == f"{expected_prefix}/v1/{signal}?tenant=example#primary"


def test_generic_grpc_endpoint_is_not_rewritten() -> None:
    endpoint = "https://otel-collector.example.com:4317/receiver?tenant=example#grpc"
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
        "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
    }

    assert (
        _exporter_endpoint(
            "traces",
            environ,
            protocol=resolve_otlp_protocol("traces", environ),
        )
        == endpoint
    )


def test_signal_specific_http_endpoint_is_not_rewritten() -> None:
    endpoint = "https://otel-collector.example.com:4318/custom?tenant=example#logs"
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel-collector.example.com:4318/base",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": endpoint,
        "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL": "http/protobuf",
    }

    assert (
        _exporter_endpoint(
            "logs",
            environ,
            protocol=resolve_otlp_protocol("logs", environ),
        )
        == endpoint
    )


def test_explicit_empty_signal_endpoint_disables_that_signal() -> None:
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.example.com:4318",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "",
    }

    assert resolve_otlp_endpoint("logs", environ, honor_sdk_disabled=True) is None
    assert (
        resolve_otlp_endpoint("traces", environ, honor_sdk_disabled=True)
        == "http://otel-collector.example.com:4318"
    )


@pytest.mark.parametrize("disabled", ("true", "TRUE", " True "))
def test_platform_service_sdk_disabled_switch_disables_export(disabled: str) -> None:
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.example.com:4318",
        "OTEL_SDK_DISABLED": disabled,
    }

    assert resolve_otlp_endpoint("traces", environ, honor_sdk_disabled=True) is None


def test_runner_scope_can_ignore_sdk_disabled_without_widening_boot_env() -> None:
    endpoint = "http://otel-collector.example.com:4318"
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
        "OTEL_SDK_DISABLED": "true",
    }

    assert resolve_otlp_endpoint("traces", environ, honor_sdk_disabled=False) == endpoint


@pytest.mark.parametrize("disabled", ("false", "0", ""))
def test_false_sdk_disabled_values_do_not_disable_export(disabled: str) -> None:
    endpoint = "http://otel-collector.example.com:4318"
    environ = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
        "OTEL_SDK_DISABLED": disabled,
    }

    assert resolve_otlp_endpoint("metrics", environ, honor_sdk_disabled=True) == endpoint


def test_unknown_signal_is_rejected() -> None:
    with pytest.raises(ValueError, match="signal"):
        resolve_otlp_endpoint("profiles", {}, honor_sdk_disabled=True)


@pytest.mark.parametrize("signal", ("traces", "logs", "metrics"))
def test_signal_protocol_uses_standard_override_precedence(signal: str) -> None:
    environ = {
        "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
        f"OTEL_EXPORTER_OTLP_{signal.upper()}_PROTOCOL": "http/protobuf",
    }

    assert resolve_otlp_protocol(signal, environ) == "http/protobuf"


@pytest.mark.parametrize("signal", ("traces", "logs", "metrics"))
@pytest.mark.parametrize("protocol", ("grpc", "http/protobuf"))
def test_each_supported_protocol_is_selected_for_each_signal(signal: str, protocol: str) -> None:
    assert (
        resolve_otlp_protocol(
            signal,
            {"OTEL_EXPORTER_OTLP_PROTOCOL": protocol},
        )
        == protocol
    )


def test_unset_or_blank_protocol_preserves_http_protobuf_default() -> None:
    assert resolve_otlp_protocol("traces", {}) == "http/protobuf"
    assert (
        resolve_otlp_protocol(
            "traces",
            {"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "  "},
        )
        == "http/protobuf"
    )


def test_unsupported_protocol_fails_closed() -> None:
    with pytest.raises(ValueError, match="unsupported OTLP protocol"):
        resolve_otlp_protocol(
            "logs",
            {"OTEL_EXPORTER_OTLP_LOGS_PROTOCOL": "http/json"},
        )
