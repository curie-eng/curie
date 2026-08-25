"""Resolution of the standard OTLP endpoint environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping

_SIGNALS = frozenset({"traces", "logs", "metrics"})
_PROTOCOLS = frozenset({"grpc", "http/protobuf"})


def resolve_otlp_endpoint(
    signal: str,
    environ: Mapping[str, str] | None = None,
    *,
    honor_sdk_disabled: bool,
) -> str | None:
    """Return the configured endpoint for one signal, or ``None`` when disabled.

    An explicitly present signal endpoint wins even when it is empty. This
    preserves Curie's empty endpoint opt out for minimal local stacks instead of
    falling back to the general endpoint or the SDK localhost default.
    """

    if signal not in _SIGNALS:
        raise ValueError(f"unsupported OTLP signal {signal!r}")
    env = os.environ if environ is None else environ
    if honor_sdk_disabled and env.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return None
    signal_key = f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT"
    raw = env[signal_key] if signal_key in env else env.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    endpoint = raw.strip()
    return endpoint or None


def resolve_otlp_protocol(
    signal: str,
    environ: Mapping[str, str] | None = None,
    *,
    default: str = "http/protobuf",
) -> str:
    """Return one supported standard OTLP protocol for ``signal``.

    The signal-specific variable has the precedence defined by the OTel SDK,
    followed by the general variable. Curie's existing HTTP/protobuf behavior
    remains the default when neither is configured.
    """

    if signal not in _SIGNALS:
        raise ValueError(f"unsupported OTLP signal {signal!r}")
    env = os.environ if environ is None else environ
    signal_key = f"OTEL_EXPORTER_OTLP_{signal.upper()}_PROTOCOL"
    raw = env[signal_key] if signal_key in env else env.get("OTEL_EXPORTER_OTLP_PROTOCOL", default)
    protocol = raw.strip().lower() or default
    if protocol not in _PROTOCOLS:
        raise ValueError(f"unsupported OTLP protocol {protocol!r}; expected grpc or http/protobuf")
    return protocol
