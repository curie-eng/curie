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
    # Discord context rules run first among the header/assignment rules so a
    # ``DISCORD_BOT_TOKEN=``/``Authorization: Bot`` credential keeps its named
    # Discord placeholder rather than falling to the generic whole-value
    # rules below (which would still consume it, just under a less specific
    # name).
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
        # A bare trailing lookahead (not just "no adjoining base64url or dot
        # segment") so a value followed by any other non-space suffix falls
        # through whole to secret_assignment below, instead of this rule
        # inserting a placeholder that trips secret_assignment's
        # ``(?!\[REDACTED:)`` guard and leaves the suffix exposed.
        re.compile(
            r"(?<![A-Za-z0-9_])(DISCORD_BOT_TOKEN=)"
            r"(?!\[REDACTED:)[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"
            r"(?!\S)"
        ),
        r"\1[REDACTED:discord_bot_token_assignment]",
    ),
    # Every whole-value context rule runs next (bearer, basic auth, DSN
    # userinfo, X-API-Key, generic secret assignment), before the Discord
    # shape rules. x_api_key and secret_assignment's guard only skips a
    # value that is already exactly one placeholder (idempotence); a
    # placeholder followed by leftover non-space text, left behind when an
    # earlier rule (e.g. channel_token) redacted just a prefix of the
    # value, still matches so the whole value is consumed here instead of
    # being exposed.
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
        "x_api_key",
        # Opaque header values have no unique prefix; match the header
        # name the mail adapter and channel clients send. The guard skips
        # only a value that IS exactly one placeholder (idempotent
        # re-run); a placeholder followed by leftover non-space text (an
        # earlier rule redacted a prefix of the value, e.g. a channel
        # token, and left a trailing suffix exposed) still matches so the
        # whole value, placeholder and suffix together, is consumed here.
        re.compile(r"(X-API-Key:\s*)(?!\[REDACTED:[a-z_]+\](?!\S))\S+", re.IGNORECASE),
        r"\1[REDACTED:x_api_key]",
    ),
    RedactionRule(
        "channel_token",
        # Runs before secret_assignment so a Curie channel token assignment
        # (``CURIE_CHANNEL_TOKEN=chn....``) keeps its named placeholder
        # instead of being claimed whole by the generic assignment rule.
        # Curie-minted ingress credential: ``chn.{payload}.{signature}``
        # (``curie_api.channel_token``, prefix ``chn``). Hyphenated
        # ``chn-{id}-{digest}`` values are event ids, not credentials.
        re.compile(r"\bchn\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
        _placeholder("channel_token"),
    ),
    RedactionRule(
        "secret_json_field",
        re.compile(
            r'(?<![A-Za-z0-9])("(?:[A-Za-z0-9]+_)*(?:secret_access_key|private_key|credential|secret|password|'
            r'passwd|pwd|api_key|apikey|access_token|token)"\s*:\s*")'
            r'(?!\[REDACTED:[a-z_]+\](?="))(?:\\.|[^"\\])*"',
            re.IGNORECASE,
        ),
        r'\1[REDACTED:secret_assignment]"',
    ),
    RedactionRule(
        "secret_dict_field",
        re.compile(
            r"(?<![A-Za-z0-9])('(?:[A-Za-z0-9]+_)*(?:secret_access_key|private_key|credential|secret|password|"
            r"passwd|pwd|api_key|apikey|access_token|token)'\s*:\s*')"
            r"(?!\[REDACTED:[a-z_]+\](?='))(?:\\.|[^'\\])*'",
            re.IGNORECASE,
        ),
        r"\1[REDACTED:secret_assignment]'",
    ),
    RedactionRule(
        "secret_colon_field",
        re.compile(
            r"(?<![A-Za-z0-9])((?:secret_access_key|private_key|credential|secret|"
            r"password|passwd|pwd|api_key|apikey|access_token|token)\s*:\s*)"
            r"(?!\[REDACTED:[a-z_]+\](?!\S))\S+",
            re.IGNORECASE,
        ),
        r"\1[REDACTED:secret_assignment]",
    ),
    RedactionRule(
        "secret_assignment",
        # ``\b`` does not fire before ``token`` in ``CURIE_CHANNEL_TOKEN=``
        # because ``_`` is a word character. Require a non-alphanumeric
        # predecessor so ``*_TOKEN=`` / ``*_SECRET=`` and private or AWS
        # secret key assignments match while
        # ``mytoken=`` does not. Keep the key name; drop only the value.
        # The guard skips only a value that IS exactly one placeholder
        # (idempotent re-run); a placeholder followed by leftover
        # non-space text (channel_token above redacted a prefix of the
        # value and left a trailing suffix exposed) still matches so the
        # whole value, placeholder and suffix together, is consumed here.
        re.compile(
            r"(?<![A-Za-z0-9])((?:secret_access_key|private_key|credential|secret|password|passwd|pwd|"
            r"api_key|apikey|access_token|token)="
            r")(?!\[REDACTED:[a-z_]+\](?!\S))\S+",
            re.IGNORECASE,
        ),
        r"\1[REDACTED:secret_assignment]",
    ),
    # The Discord shape rules run after the whole-value context rules above
    # so a token that context has already redacted whole is not re-matched,
    # then before the generic prefix rules (api_key, github_pat,
    # slack_token, etc.) so a token or webhook token segment that happens to
    # contain an ``am_``/``sk-`` style prefix is matched whole by the
    # Discord rule first, rather than being partly consumed by a narrower
    # prefix rule.
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
        "api_key",
        # sk-/xai- are the in-tree model-key prefixes. AgentMail documents
        # ``am_`` (https://docs.agentmail.to/knowledge-base/getting-api-key.md).
        re.compile(r"\b(?:(?:sk|xai)[-_]|am_)[A-Za-z0-9_-]{16,}"),
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
