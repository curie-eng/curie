"""Service logs remain JSON stderr while exporting correlated redacted records."""

from __future__ import annotations

import io
import json
import logging

import pytest
from curie_telemetry import build_resource, configure_service_logging
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.trace import TracerProvider

_SECRET_VECTORS = (
    "sk-" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE0000",
    "AKIA" + "EXAMPLEFAKEKEY0000",
    "ghp_" + "0000FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE",
    "glpat-" + "0000FAKEFAKEFAKEFAKE",
    "xoxb-" + "0000000000-0000000000-FAKEFAKEFAKEFAKEFAKEFAKE",
    "xapp-" + "0-0000000000-0000000000-FAKEFAKEFAKEFAKEFAKEFAKE",
    "AIza" + "SyFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE0",
    (
        "-----BEGIN " + "RSA PRIVATE KEY-----\n"
        "MIIFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE0000\n"
        "-----END " + "RSA PRIVATE KEY-----"
    ),
    "Bearer " + "abc0000FAKEFAKEFAKEFAKEFAKEFAKE",
    "Authorization: Basic " + "ZmFrZS11c2VyOmZha2UtcGFzc3dvcmQ=",
    "postgresql://fake-user:" + "fake-password@db.example.com:5432/acme",
    "eyJ" + "hbGciOiJIUzI1NiJ9.eyJzdWIiOiJmYWtlIn0.FAKEFAKEFAKEFAKE00",
    "https://example.invalid/hook?token=" + "0000FAKEFAKEFAKEFAKE",
    "secret=" + "0000FAKEFAKEFAKEVALUE",
    "/home/example-user/.config/curie/settings.json",
)


def _body(exported: object) -> str:
    log_record = getattr(exported, "log_record", exported)
    return str(log_record.body)


def _trace_ids(exported: object) -> tuple[int, int]:
    log_record = getattr(exported, "log_record", exported)
    return int(log_record.trace_id), int(log_record.span_id)


def _attributes(exported: object) -> dict[str, object]:
    log_record = getattr(exported, "log_record", exported)
    return dict(log_record.attributes or {})


class _RecordCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_idempotent_reconfiguration_rebinds_stderr_instead_of_using_a_closed_stream() -> None:
    logger = logging.getLogger("curie.telemetry.stderr-rebind")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    first = io.StringIO()
    second = io.StringIO()
    try:
        configure_service_logging(logger, service_name="curie-dispatcher", stream=first)
        first.close()
        configure_service_logging(logger, service_name="curie-dispatcher", stream=second)

        logger.error("safe diagnostic")

        assert "safe diagnostic" in second.getvalue()
        assert len(
            [
                handler
                for handler in logger.handlers
                if getattr(handler, "_curie_json_service", None) == "curie-dispatcher"
            ]
        ) == 1
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate


def test_existing_propagated_capture_survives_json_and_otlp_configuration() -> None:
    redaction_probe = "sk-" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE1111"
    resource = build_resource(
        "curie-worker",
        service_version="0.7.0",
        service_instance_id="acme-worker-capture",
        deployment_environment="test",
    )
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    stderr = io.StringIO()
    capture = _RecordCapture()
    root = logging.getLogger()
    logger = logging.getLogger("curie.telemetry.capture-preservation")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    had_preserve_marker = hasattr(logger, "_curie_preserve_propagated_handlers")
    original_preserve_marker = getattr(logger, "_curie_preserve_propagated_handlers", None)

    root.addHandler(capture)
    logger.handlers.clear()
    logger.propagate = True
    try:
        configure_service_logging(
            logger,
            service_name="curie-worker",
            stream=stderr,
            logger_provider=logger_provider,
        )
        # This deliberately drives a fake credential through the public logging
        # seam and asserts both stderr and OTLP receive only its redacted form.
        logger.error(
            "dead-letter alert credential=%s",
            redaction_probe,
        )

        assert logger.propagate is False
        assert sum(
            bool(getattr(handler, "_curie_dynamic_propagation", False))
            for handler in logger.handlers
        ) == 1
        assert len(capture.records) == 1
        assert capture.records[0].name == logger.name
        assert redaction_probe not in capture.records[0].getMessage()
        assert "[REDACTED:" in capture.records[0].getMessage()

        stderr_record = json.loads(stderr.getvalue())
        assert stderr_record["logger"] == logger.name
        assert redaction_probe not in stderr_record["message"]

        (exported,) = exporter.get_finished_logs()
        assert redaction_probe not in _body(exported)
        assert "[REDACTED:" in _body(exported)
    finally:
        root.removeHandler(capture)
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        if had_preserve_marker:
            logger._curie_preserve_propagated_handlers = original_preserve_marker  # type: ignore[attr-defined]
        else:
            del logger._curie_preserve_propagated_handlers  # type: ignore[attr-defined]
        logger_provider.shutdown()


def test_root_capture_replacement_does_not_retain_or_duplicate_old_handler() -> None:
    first = _RecordCapture()
    second = _RecordCapture()
    root = logging.getLogger()
    logger = logging.getLogger("curie.telemetry.capture-lifecycle")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    had_preserve_marker = hasattr(logger, "_curie_preserve_propagated_handlers")
    original_preserve_marker = getattr(logger, "_curie_preserve_propagated_handlers", None)
    logger.handlers.clear()
    logger.propagate = True
    root.addHandler(first)
    try:
        configure_service_logging(logger, service_name="curie-worker", stream=io.StringIO())
        logger.warning("first lifecycle")
        root.removeHandler(first)
        root.addHandler(second)
        logger.addHandler(second)

        logger.warning("second lifecycle")

        assert [record.getMessage() for record in first.records] == ["first lifecycle"]
        assert [record.getMessage() for record in second.records] == ["second lifecycle"]
    finally:
        if first in root.handlers:
            root.removeHandler(first)
        if second in root.handlers:
            root.removeHandler(second)
        if second in logger.handlers:
            logger.removeHandler(second)
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        if had_preserve_marker:
            logger._curie_preserve_propagated_handlers = original_preserve_marker  # type: ignore[attr-defined]
        else:
            del logger._curie_preserve_propagated_handlers  # type: ignore[attr-defined]


def test_exception_only_secret_is_redacted_for_preserved_root_handler() -> None:
    redaction_probe = "sk-" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE2222"
    stderr = io.StringIO()
    inherited_stream = io.StringIO()
    inherited = logging.StreamHandler(inherited_stream)
    inherited.setFormatter(logging.Formatter("%(message)s %(context)s"))
    root = logging.getLogger()
    logger = logging.getLogger("curie.telemetry.propagated-exception-redaction")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    had_preserve_marker = hasattr(logger, "_curie_preserve_propagated_handlers")
    original_preserve_marker = getattr(logger, "_curie_preserve_propagated_handlers", None)
    root.addHandler(inherited)
    logger.handlers.clear()
    logger.propagate = True
    try:
        configure_service_logging(logger, service_name="curie-api", stream=stderr)
        try:
            raise RuntimeError(f"exception-only credential {redaction_probe}")
        except RuntimeError:
            # The call is the redaction seam under test, not a cleartext sink.
            logger.exception(
                "request failed",
                # Intentional redaction regression; no cleartext reaches a sink.
                extra={
                    "context": {"authorization": redaction_probe}
                },
            )

        for output in (stderr.getvalue(), inherited_stream.getvalue()):
            assert "RuntimeError" in output
            assert redaction_probe not in output
            assert "[REDACTED:" in output
    finally:
        root.removeHandler(inherited)
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        if had_preserve_marker:
            logger._curie_preserve_propagated_handlers = original_preserve_marker  # type: ignore[attr-defined]
        else:
            del logger._curie_preserve_propagated_handlers  # type: ignore[attr-defined]


@pytest.mark.parametrize("redaction_probe", _SECRET_VECTORS)
def test_args_style_service_log_is_correlated_redacted_and_preserved_on_stderr(
    redaction_probe: str,
) -> None:
    resource = build_resource(
        "curie-worker",
        service_version="0.7.0",
        service_instance_id="acme-worker-instance",
        deployment_environment="test",
    )
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    stderr = io.StringIO()
    logger = logging.getLogger(
        f"curie.telemetry.test.{len(redaction_probe)}.{redaction_probe[:4]}"
    )
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate

    try:
        configured = configure_service_logging(
            logger,
            service_name="curie-worker",
            stream=stderr,
            logger_provider=logger_provider,
        )
        assert configured is logger
        assert (
            configure_service_logging(
                logger,
                service_name="curie-worker",
                stream=stderr,
                logger_provider=logger_provider,
            )
            is logger
        )
        tracer = TracerProvider().get_tracer("curie-telemetry-tests")
        with tracer.start_as_current_span("worker.process") as span:
            expected_trace_id = span.get_span_context().trace_id
            expected_span_id = span.get_span_context().span_id
            # Intentional redaction regression; no cleartext reaches a sink.
            logger.error(
                "runner request failed value=%s",
                redaction_probe,
            )

        line = stderr.getvalue().strip()
        assert line, "the OTLP handler must not replace the existing stderr path"
        assert len(line.splitlines()) == 1
        stderr_record = json.loads(line)
        assert stderr_record["service.name"] == "curie-worker"
        assert stderr_record["severity"] == "ERROR"
        assert stderr_record["trace_id"] == f"{expected_trace_id:032x}"
        assert stderr_record["span_id"] == f"{expected_span_id:016x}"
        assert redaction_probe not in stderr_record["message"]
        assert "[REDACTED:" in stderr_record["message"]

        (exported,) = exporter.get_finished_logs()
        assert redaction_probe not in _body(exported)
        assert "[REDACTED:" in _body(exported)
        assert _trace_ids(exported) == (expected_trace_id, expected_span_id)
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        logger_provider.shutdown()


def test_exception_keeps_redacted_stderr_traceback_but_closes_otlp_fields() -> None:
    redaction_probe = "sk-" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE3333"
    resource = build_resource(
        "curie-api",
        service_version="0.7.0",
        service_instance_id="acme-api-exception",
        deployment_environment="test",
    )
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    stderr = io.StringIO()
    logger = logging.getLogger("curie.telemetry.exception-redaction")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate

    try:
        configure_service_logging(
            logger,
            service_name="curie-api",
            stream=stderr,
            logger_provider=logger_provider,
        )
        try:
            raise RuntimeError(f"request failed credential={redaction_probe}")
        except RuntimeError:
            # Intentional redaction regression; no cleartext reaches a sink.
            logger.exception(
                "request handler escaped credential=%s",
                redaction_probe,
            )

        stderr_record = json.loads(stderr.getvalue())
        assert stderr_record["severity"] == "ERROR"
        assert "Traceback (most recent call last)" in stderr_record["exception"]
        assert "RuntimeError" in stderr_record["exception"]
        assert redaction_probe not in stderr_record["message"]
        assert redaction_probe not in stderr_record["exception"]
        assert "[REDACTED:" in stderr_record["exception"]

        (exported,) = exporter.get_finished_logs()
        assert redaction_probe not in _body(exported)
        attributes = _attributes(exported)
        assert attributes["exception.type"] == "RuntimeError"
        assert "exception.message" not in attributes
        assert "exception.stacktrace" not in attributes
        assert redaction_probe not in repr(attributes)
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        logger_provider.shutdown()


def test_otlp_copy_drops_dynamic_identifiers_and_unbounded_extra_fields() -> None:
    resource = build_resource(
        "curie-runner",
        service_version="0.7.0",
        service_instance_id="acme-runner-log-bounds",
    )
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    stderr = io.StringIO()
    logger = logging.getLogger("curie.telemetry.dynamic-identifier-bound")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    dynamic_id = "session-example-dynamic-identifier"
    oversized = "X" * 100_000
    try:
        configure_service_logging(
            logger,
            service_name="curie-runner",
            stream=stderr,
            logger_provider=logger_provider,
        )
        logger.info("session %s active", dynamic_id, extra={"unsafe.extra": oversized})
        logger.info(oversized)

        stderr_text = stderr.getvalue()
        assert dynamic_id in stderr_text
        assert oversized in stderr_text

        first, second = exporter.get_finished_logs()
        assert _body(first) == "session %s active"
        assert dynamic_id not in _body(first)
        assert "unsafe.extra" not in _attributes(first)
        assert len(_body(second)) <= 4096 + len("[TRUNCATED]")
        assert oversized not in _body(second)
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        logger_provider.shutdown()


def test_json_redaction_does_not_expand_the_otlp_body_after_reconfiguration() -> None:
    """A stderr-only identifier stays out of OTLP even when a secret redacts."""

    resource = build_resource(
        "curie-worker",
        service_version="0.7.0",
        service_instance_id="acme-worker-log-privacy",
    )
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    stderr = io.StringIO()
    logger = logging.getLogger("curie.telemetry.handler-order-privacy")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    dynamic_id = "session-nonsecret-dynamic-identifier"
    credential = "sk-" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE4444"
    try:
        configure_service_logging(
            logger,
            service_name="curie-worker",
            stream=stderr,
            logger_provider=logger_provider,
        )
        # The second call must keep the same handler topology and privacy
        # boundary. Before this regression, stderr's mutable filter formatted
        # both args first, so the OTLP sibling received ``dynamic_id``.
        configure_service_logging(
            logger,
            service_name="curie-worker",
            stream=stderr,
            logger_provider=logger_provider,
        )

        # Intentional redaction regression; no cleartext reaches a sink.
        logger.error("session %s failed credential=%s", dynamic_id, credential)

        stderr_record = json.loads(stderr.getvalue())
        assert dynamic_id in stderr_record["message"]
        assert credential not in stderr_record["message"]
        assert "[REDACTED:" in stderr_record["message"]

        (exported,) = exporter.get_finished_logs()
        assert dynamic_id not in _body(exported)
        assert credential not in _body(exported)
        assert _body(exported) == "session %s failed credential=[REDACTED:api_key]"
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        logger_provider.shutdown()


def test_otlp_copy_redacts_source_path_while_stderr_remains_available() -> None:
    resource = build_resource(
        "curie-api",
        service_version="0.7.0",
        service_instance_id="acme-api-path-redaction",
    )
    exporter = InMemoryLogRecordExporter()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    stderr = io.StringIO()
    logger = logging.getLogger("curie.telemetry.source-path-redaction")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    private_path = "/home/real-user/private/app.py"
    try:
        configure_service_logging(
            logger,
            service_name="curie-api",
            stream=stderr,
            logger_provider=logger_provider,
        )
        record = logger.makeRecord(
            logger.name,
            logging.ERROR,
            private_path,
            7,
            "request failed",
            (),
            None,
        )
        logger.handle(record)

        assert "request failed" in stderr.getvalue()
        (exported,) = exporter.get_finished_logs()
        attributes = _attributes(exported)
        assert private_path not in repr(attributes)
        assert attributes["code.file.path"] == "[REDACTED:home_path]/private/app.py"
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
        logger_provider.shutdown()
