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
    # The four Discord rules run before the prefix rules (api_key, github_pat,
    # slack_token, etc.) so a token or webhook token segment that happens to
    # contain an ``am_``/``sk-`` style prefix is matched whole by the Discord
    # rule first, rather than being partly consumed by a narrower prefix rule.
    RedactionRule(
        "discord_bot_authorization",
        # Discord's API reference demonstrates bot credentials in an
        # ``Authorization: Bot <token>`` header:
        # https://docs.discord.com/developers/reference
        re.compile(
            r"(?<![A-Za-z0-9_-])(Authorization:\s+Bot\s+)(?!\[REDACTED:)"
            r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"
            r"(?![A-Za-z0-9_-]|\.[A-Za-z0-9_-]+)",
            re.IGNORECASE,
        ),
        r"\1[REDACTED:discord_bot_authorization]",
    ),
    RedactionRule(
        "discord_bot_token_assignment",
        re.compile(
            r"(?<![A-Za-z0-9_])(DISCORD_BOT_TOKEN=)"
            r"(?!\[REDACTED:)[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"
            r"(?![A-Za-z0-9_-]|\.[A-Za-z0-9_-]+)"
        ),
        r"\1[REDACTED:discord_bot_token_assignment]",
    ),
    RedactionRule(
        "discord_webhook_url",
        # Discord executes webhooks at ``/webhooks/{webhook.id}/{webhook.token}``
        # and the token alone authorizes posting:
        # https://docs.discord.com/developers/resources/webhook
        # Scheme, host, path and id stay diagnostic; only the token is dropped.
        re.compile(
            r"(https?://(?:(?:ptb|canary)\.)?discord(?:app)?\.com/api/(?:v\d+/)?webhooks/\d+/)"
            r"(?!\[REDACTED:)[A-Za-z0-9_-]+",
            re.IGNORECASE,
        ),
        r"\1[REDACTED:discord_webhook_url]",
    ),
    RedactionRule(
        "discord_bot_token",
        # Shape rule for a bot token with no surrounding context (a dict repr,
        # a bare value). Tokens are ``base64(user id).timestamp.hmac``; a
        # snowflake id starts with 1-3, which base64-encodes to M, N or O. This
        # is the shape TruffleHog's Discord detector keys on
        # (https://github.com/trufflesecurity/trufflehog, pkg/detectors/discordbottoken),
        # with the first segment widened to 28 chars for 19-20 digit ids. It
        # runs after the context rules so those keep their named placeholders.
        # Neither side may continue into another dotted segment.
        re.compile(
            r"(?<![A-Za-z0-9_-])(?<![A-Za-z0-9_-]\.)"
            r"[MNO][A-Za-z0-9_-]{23,27}\.[A-Za-z0-9_-]{6}\.[A-Za-z0-9_-]{27,38}"
            r"(?![A-Za-z0-9_-]|\.[A-Za-z0-9_-])"
        ),
        _placeholder("discord_bot_token"),
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
        # sk-/xai- are the in-tree model-key prefixes. AgentMail documents
        # ``am_`` (https://docs.agentmail.to/knowledge-base/getting-api-key.md).
        re.compile(r"\b(?:(?:sk|xai)[-_]|am_)[A-Za-z0-9_-]{16,}"),
        _placeholder("api_key"),
    ),
    RedactionRule(
        "channel_token",
        # Curie-minted ingress credential: ``chn.{payload}.{signature}``
        # (``curie_api.channel_token``, prefix ``chn``). Hyphenated
        # ``chn-{id}-{digest}`` values are event ids, not credentials.
        re.compile(r"\bchn\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
        _placeholder("channel_token"),
    ),
    RedactionRule(
        "x_api_key",
        # Opaque header values have no unique prefix; match the header
        # name the mail adapter and channel clients send.
        re.compile(r"(X-API-Key:\s*)(?!\[REDACTED:)\S+", re.IGNORECASE),
        r"\1[REDACTED:x_api_key]",
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
        # ``\b`` does not fire before ``token`` in ``CURIE_CHANNEL_TOKEN=``
        # because ``_`` is a word character. Require a non-alphanumeric
        # predecessor so ``*_TOKEN=`` / ``*_SECRET=`` match while
        # ``mytoken=`` does not. Keep the key name; drop only the value.
        re.compile(
            r"(?<![A-Za-z0-9])((?:secret|password|passwd|pwd|api_key|apikey|access_token|token)=)(?!\[REDACTED:)\S+",
            re.IGNORECASE,
        ),
        r"\1[REDACTED:secret_assignment]",
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
