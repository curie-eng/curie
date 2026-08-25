"""Shared secret redaction for Curie logs and span attributes."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RedactionRule:
    name: str
    pattern: re.Pattern[str]
    placeholder: str


def _placeholder(name: str) -> str:
    return f"[REDACTED:{name}]"


REDACTION_RULES: tuple[RedactionRule, ...] = (
    RedactionRule(
        "pem_private_key",
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
        _placeholder("pem_private_key"),
    ),
    RedactionRule(
        "url_secret_param",
        re.compile(
            r"[?&](?:token|secret|password|passwd|pwd|api_key|apikey|access_token|key|sig"
            r"|signature)=[^&\s]+",
            re.IGNORECASE,
        ),
        _placeholder("url_secret_param"),
    ),
    RedactionRule(
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
        _placeholder("jwt"),
    ),
    RedactionRule(
        "bearer_token",
        re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+"),
        _placeholder("bearer_token"),
    ),
    RedactionRule(
        "basic_auth",
        re.compile(r"\bBasic\s+[A-Za-z0-9+/=]+", re.IGNORECASE),
        _placeholder("basic_auth"),
    ),
    RedactionRule(
        "dsn_userinfo",
        re.compile(
            r"(\b[a-z][a-z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@",
            re.IGNORECASE,
        ),
        r"\1[REDACTED:dsn_userinfo]@",
    ),
    RedactionRule(
        "api_key",
        re.compile(r"\b(?:sk|xai)[-_][A-Za-z0-9_-]{16,}"),
        _placeholder("api_key"),
    ),
    RedactionRule(
        "aws_access_key_id",
        re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16,}"),
        _placeholder("aws_access_key_id"),
    ),
    RedactionRule(
        "github_pat",
        re.compile(r"\b(?:github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,})"),
        _placeholder("github_pat"),
    ),
    RedactionRule(
        "gitlab_token",
        re.compile(r"\bglpat-[A-Za-z0-9_-]{16,}"),
        _placeholder("gitlab_token"),
    ),
    RedactionRule(
        "slack_token",
        re.compile(r"\b(?:xox[baps]|xapp)-[A-Za-z0-9-]{10,}"),
        _placeholder("slack_token"),
    ),
    RedactionRule(
        "google_api_key",
        re.compile(r"\bAIza[A-Za-z0-9_-]{30,}"),
        _placeholder("google_api_key"),
    ),
    RedactionRule(
        "secret_assignment",
        re.compile(
            r"\b(?:secret|password|passwd|pwd|api_key|apikey|access_token|token)=\S+",
            re.IGNORECASE,
        ),
        _placeholder("secret_assignment"),
    ),
    RedactionRule(
        "home_path",
        re.compile(r"/(?:home|Users)/[^/\s]+"),
        _placeholder("home_path"),
    ),
)

REDACTION_BOUNDARIES: tuple[str, ...] = ("stdout", "gen_ai_span")


def redact_text(text: str) -> str:
    for rule in REDACTION_RULES:
        text = rule.pattern.sub(rule.placeholder, text)
    return text


def redact_span_attribute(value: object) -> object:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, (list, tuple)):
        scrubbed = [redact_span_attribute(item) for item in value]
        return tuple(scrubbed) if isinstance(value, tuple) else scrubbed
    return value


class RedactingLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = redact_text(message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


def install_stdout_redaction() -> None:
    """Keep the runner compatibility hook while sharing one policy."""

    for handler in logging.getLogger().handlers:
        if not any(isinstance(item, RedactingLogFilter) for item in handler.filters):
            handler.addFilter(RedactingLogFilter())
