"""JSON stderr and correlated OpenTelemetry LogRecords."""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Mapping
from copy import copy
from datetime import UTC, datetime
from typing import IO, Any

from opentelemetry import trace
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler

from .redact import RedactingLogFilter, redact_text

_MAX_OTLP_BODY_LENGTH = 4096
_MAX_OTLP_ATTRIBUTE_LENGTH = 256
_MAX_OTLP_PATH_LENGTH = 512
_STANDARD_RECORD_KEYS = frozenset(
    logging.LogRecord("", logging.INFO, "", 0, "", (), None).__dict__
)
_SAFE_EXTRA_KEYS = frozenset({"exception.type"})


def _bounded(text: str, limit: int) -> str:
    return text if len(text) <= limit else f"{text[:limit]}[TRUNCATED]"


class _JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        span_context = trace.get_current_span().get_span_context()
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "service.name": self._service_name,
            "severity": record.levelname,
            "logger": record.name,
            "message": redact_text(record.getMessage()),
            "trace_id": f"{span_context.trace_id:032x}" if span_context.is_valid else None,
            "span_id": f"{span_context.span_id:016x}" if span_context.is_valid else None,
        }
        if record.exc_info is not None:
            payload["exception"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


_PERCENT_FORMAT = re.compile(
    r"%(?:\((?P<key>[^)]+)\))?[#0\- +]?(?:\d+|\*)?(?:\.(?:\d+|\*))?[hlL]?[diouxXeEfFgGcrs]"
)


def _otlp_body(record: logging.LogRecord) -> str:
    """Keep dynamic args out while retaining only redaction markers for secrets."""

    template = str(record.msg)
    args = record.args
    positional = args if isinstance(args, tuple) else (args,)
    index = 0

    def replacement(match: re.Match[str]) -> str:
        nonlocal index
        key = match.group("key")
        if key is not None:
            if not isinstance(args, Mapping) or key not in args:
                return match.group(0)
            value = args[key]
        else:
            if index >= len(positional):
                return match.group(0)
            value = positional[index]
            index += 1
        rendered = str(value)
        redacted = redact_text(rendered)
        # A safe formatting arg remains the original placeholder. A secret may
        # contribute its bounded marker but never its neighboring dynamic args.
        return redacted if redacted != rendered else match.group(0)

    return _bounded(
        redact_text(_PERCENT_FORMAT.sub(replacement, template)), _MAX_OTLP_BODY_LENGTH
    )


def _otlp_safe_record(record: logging.LogRecord) -> logging.LogRecord:
    """Give OTLP a safe record without mutating stderr traceback diagnostics."""

    safe_record = copy(record)
    safe_record.msg = _otlp_body(record)
    safe_record.args = ()
    safe_record.pathname = _bounded(
        redact_text(record.pathname), _MAX_OTLP_PATH_LENGTH
    )
    exception_type = record.exc_info[0] if record.exc_info is not None else None
    safe_record.exc_info = None
    safe_record.exc_text = None
    for key in tuple(safe_record.__dict__):
        if key not in _STANDARD_RECORD_KEYS and key not in _SAFE_EXTRA_KEYS:
            del safe_record.__dict__[key]
    if exception_type is not None:
        safe_record.__dict__["exception.type"] = _bounded(
            exception_type.__name__, _MAX_OTLP_ATTRIBUTE_LENGTH
        )
    return safe_record


class _OtelRedactingLogHandler(LoggingHandler):
    """Hand the OTLP exporter an isolated, bounded log record."""

    def handle(self, record: logging.LogRecord) -> bool:
        return super().handle(_otlp_safe_record(record))


def _redacted_diagnostic_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {
            _redacted_diagnostic_value(key): _redacted_diagnostic_value(item)
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(_redacted_diagnostic_value(item) for item in value)
    if isinstance(value, list):
        return [_redacted_diagnostic_value(item) for item in value]
    if isinstance(value, set):
        return {_redacted_diagnostic_value(item) for item in value}
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact_text(str(value))


def _redacted_diagnostic_record(record: logging.LogRecord) -> logging.LogRecord:
    """Clone a full diagnostic record without leaving secret-bearing fields."""

    safe_record = copy(record)
    safe_record.msg = redact_text(record.getMessage())
    safe_record.args = ()
    if record.exc_info is not None:
        traceback = logging.Formatter().formatException(record.exc_info)
        safe_record.exc_text = redact_text(traceback)
        safe_record.exc_info = None
    elif record.exc_text is not None:
        safe_record.exc_text = redact_text(record.exc_text)
    for key, value in tuple(safe_record.__dict__.items()):
        safe_record.__dict__[key] = _redacted_diagnostic_value(value)
    return safe_record


class _RedactingJsonHandler(logging.StreamHandler[IO[str]]):
    """Format a redacted diagnostic copy without changing sibling handlers' record."""

    def handle(self, record: logging.LogRecord) -> bool:
        # ``RedactingLogFilter`` formats ``record.args`` when it finds a
        # secret. Handlers receive the same LogRecord, so apply it only to this
        # copy and retain the exception state for JSON stderr diagnostics.
        return super().handle(copy(record))


class _RedactingRootHandlerProxy(logging.Handler):
    """Safely mirror to the root handlers that exist when a record is emitted."""

    _curie_dynamic_propagation = True

    def __init__(self, owner: logging.Logger) -> None:
        super().__init__()
        self._owner = owner

    def emit(self, record: logging.LogRecord) -> None:
        # Test runners and embedders replace root capture handlers between
        # lifecycles. Resolving them here avoids retaining a closed or stale
        # handler and emitting the same record through multiple generations.
        safe_record = _redacted_diagnostic_record(record)
        for delegate in tuple(logging.getLogger().handlers):
            if delegate is self or getattr(delegate, "_curie_dynamic_propagation", False):
                continue
            # pytest and embedders may temporarily attach the same capture
            # handler to the named logger and root. Logger.callHandlers already
            # invoked the direct copy, so mirroring it again would duplicate.
            if delegate in self._owner.handlers:
                continue
            if safe_record.levelno >= delegate.level:
                delegate.handle(copy(safe_record))


def configure_service_logging(
    logger: logging.Logger,
    *,
    service_name: str,
    stream: IO[str] | None = None,
    logger_provider: LoggerProvider | None = None,
    level: int | str = logging.INFO,
) -> logging.Logger:
    """Configure one service logger idempotently."""

    preserve_propagated = getattr(logger, "_curie_preserve_propagated_handlers", logger.propagate)
    logger._curie_preserve_propagated_handlers = preserve_propagated  # type: ignore[attr-defined]
    logger.setLevel(level)
    logger.propagate = False
    json_handler = next(
        (
            handler
            for handler in logger.handlers
            if getattr(handler, "_curie_json_service", None) == service_name
        ),
        None,
    )
    if json_handler is not None and not isinstance(
        json_handler, _RedactingJsonHandler
    ):
        # A logger can be reconfigured in-process after an earlier bootstrap.
        # Replace the legacy mutable-filter handler so the ordering guarantee
        # holds across idempotent reconfiguration too.
        logger.removeHandler(json_handler)
        json_handler.close()
        json_handler = None
    if json_handler is None:
        json_handler = _RedactingJsonHandler(stream or sys.stderr)
        json_handler.setFormatter(_JsonFormatter(service_name))
        json_handler.addFilter(RedactingLogFilter())
        json_handler._curie_json_service = service_name  # type: ignore[attr-defined]
        logger.addHandler(json_handler)
    elif isinstance(json_handler, logging.StreamHandler):
        # Test runners and embedders replace ``sys.stderr`` between lifecycles.
        # Reusing a handler bound to the prior (now closed) capture stream makes
        # logging's own emergency traceback expose the exception we deliberately
        # redacted. Assign directly rather than ``setStream`` because that method
        # flushes the already-closed stream before swapping it.
        json_handler.stream = stream or sys.stderr

    existing_otel = [
        handler for handler in logger.handlers if getattr(handler, "_curie_otel_handler", False)
    ]
    wanted_provider = id(logger_provider) if logger_provider is not None else None
    for handler in existing_otel:
        if getattr(handler, "_curie_logger_provider", None) != wanted_provider or not isinstance(
            handler, _OtelRedactingLogHandler
        ):
            logger.removeHandler(handler)
            handler.close()
    if logger_provider is not None and not any(
        getattr(handler, "_curie_logger_provider", None) == wanted_provider
        for handler in logger.handlers
    ):
        otel_handler = _OtelRedactingLogHandler(
            level=logging.NOTSET, logger_provider=logger_provider
        )
        otel_handler._curie_otel_handler = True  # type: ignore[attr-defined]
        otel_handler._curie_logger_provider = wanted_provider  # type: ignore[attr-defined]
        logger.addHandler(otel_handler)
    # Mirror to the current root capture/diagnostic handlers through one
    # cloning proxy. A fixed delegate becomes stale across application/test
    # lifecycles and duplicates later records; resolving at emit time preserves
    # both idempotence and exception redaction.
    propagation_proxies = [
        handler
        for handler in logger.handlers
        if getattr(handler, "_curie_dynamic_propagation", False)
    ]
    if preserve_propagated and not propagation_proxies:
        logger.addHandler(_RedactingRootHandlerProxy(logger))
    for proxy in propagation_proxies[1 if preserve_propagated else 0 :]:
        logger.removeHandler(proxy)
        proxy.close()
    return logger
