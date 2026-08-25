"""Compatibility exports for the shared Curie telemetry redaction policy."""

from curie_telemetry.redact import (
    REDACTION_BOUNDARIES,
    REDACTION_RULES,
    RedactingLogFilter,
    RedactionRule,
    install_stdout_redaction,
    redact_span_attribute,
    redact_text,
)

__all__ = [
    "REDACTION_BOUNDARIES",
    "REDACTION_RULES",
    "RedactingLogFilter",
    "RedactionRule",
    "install_stdout_redaction",
    "redact_span_attribute",
    "redact_text",
]
