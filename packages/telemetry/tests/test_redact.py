"""Redaction closes authentication formats before any telemetry export."""

from __future__ import annotations

import io
import logging

from curie_telemetry.redact import RedactingLogFilter, redact_text

# Hoisted synthetic values. The check-secrets hook false-positives on inline
# token literals, so prefixes are split rather than written at the call site.
#
# Channel tokens are minted as ``chn.{payload}.{signature}``
# (``apps/api/src/curie_api/channel_token.py``, prefix ``chn``). The issue's
# ``chn-{channel_id}-{digest}`` shape is ``_event_id``, not a credential.
FAKE_CHANNEL_TOKEN = "chn." + "ZXhhbXBsZWNoYW5uZWxwYXlsb2Fk." + "FAKEFAKEFAKESIG0000"
# Provider docs: "API keys start with `am_`"
# https://docs.agentmail.to/knowledge-base/getting-api-key.md
FAKE_AGENTMAIL_API_KEY = "am_" + "FAKEFAKEFAKEFAKEFAKE0000"
FAKE_EGRESS_SECRET = "FAKEFAKEFAKEEGRESS0000"
FAKE_HEADER_VALUE = "FAKEFAKEFAKEHEADERVALUE0000"
# Discord documents ``Authorization: Bot id.timestamp.hmac``:
# https://docs.discord.com/developers/reference
# These synthetic segments mirror its example's 24/6/27 lengths only to keep
# the positive representative; the lengths are not part of the asserted grammar.
FAKE_DISCORD_BOT_TOKEN = (
    "FAKEFAKEFAKEFAKEFAKE0000." + "FAKE00." + "FAKEFAKEFAKEFAKEFAKEFAKE000"
)
FAKE_DISCORD_BOT_AUTHORIZATION = "Authorization: Bot " + FAKE_DISCORD_BOT_TOKEN
FAKE_DISCORD_BOT_TOKEN_ASSIGNMENT = "DISCORD_BOT_TOKEN=" + FAKE_DISCORD_BOT_TOKEN
# Delivery correlation id from the channel protocol tests, not a credential.
BENIGN_EVENT_ID = "chn-7f3-a1b2c3d4e5f60718"


def test_basic_authorization_value_is_removed() -> None:
    encoded = "ZmFrZS11c2VyOmZha2UtcGFzc3dvcmQ="

    redacted = redact_text(f"Authorization: Basic {encoded}")

    assert encoded not in redacted
    assert "Authorization: [REDACTED:basic_auth]" == redacted


def test_dsn_userinfo_is_removed_while_host_and_database_remain_diagnostic() -> None:
    password = "fake-password"

    redacted = redact_text(
        f"connection failed postgresql://fake-user:{password}@db.example.com:5432/acme"
    )

    assert "fake-user" not in redacted
    assert password not in redacted
    assert "postgresql://[REDACTED:dsn_userinfo]@db.example.com:5432/acme" in redacted


def test_slack_app_level_token_is_removed() -> None:
    token = "xapp-0-0000000000-0000000000-FAKEFAKEFAKEFAKE"

    redacted = redact_text(f"app credential {token}")

    assert token not in redacted
    assert "[REDACTED:slack_token]" in redacted


def test_bare_channel_token_shape_is_redacted() -> None:
    redacted = redact_text(f"adapter credential {FAKE_CHANNEL_TOKEN}")

    assert FAKE_CHANNEL_TOKEN not in redacted
    assert "[REDACTED:channel_token]" in redacted
    assert "adapter credential " in redacted


def test_channel_event_id_is_not_treated_as_a_credential() -> None:
    line = f"inbound admitted event_id={BENIGN_EVENT_ID}"

    assert redact_text(line) == line


def test_curie_channel_token_assignment_is_redacted() -> None:
    value = FAKE_HEADER_VALUE
    redacted = redact_text(f"boot CURIE_CHANNEL_TOKEN={value} ready")

    assert value not in redacted
    assert "CURIE_CHANNEL_TOKEN=" in redacted
    assert "[REDACTED:secret_assignment]" in redacted
    assert "boot " in redacted
    assert " ready" in redacted


def test_curie_egress_secret_assignment_is_redacted() -> None:
    redacted = redact_text(f"boot CURIE_EGRESS_SECRET={FAKE_EGRESS_SECRET} ready")

    assert FAKE_EGRESS_SECRET not in redacted
    assert "CURIE_EGRESS_SECRET=" in redacted
    assert "[REDACTED:secret_assignment]" in redacted


def test_unprefixed_secret_without_assignment_or_header_context_is_not_redacted() -> None:
    # CURIE_EGRESS_SECRET has no unique prefix. A regex that claimed to
    # recognize the value itself would also redact ordinary diagnostic text.
    line = f"retry after {FAKE_EGRESS_SECRET} milliseconds"

    assert redact_text(line) == line


def test_discord_bot_authorization_is_redacted() -> None:
    redacted = redact_text(
        f"request used {FAKE_DISCORD_BOT_AUTHORIZATION} and was rejected"
    )

    assert FAKE_DISCORD_BOT_TOKEN not in redacted
    assert redacted == (
        "request used Authorization: Bot "
        "[REDACTED:discord_bot_authorization] and was rejected"
    )


def test_discord_bot_authorization_is_matched_case_insensitively() -> None:
    mixed_case_authorization = "aUtHoRiZaTiOn: bOt " + FAKE_DISCORD_BOT_TOKEN

    redacted = redact_text(f"request used {mixed_case_authorization} and was rejected")

    assert FAKE_DISCORD_BOT_TOKEN not in redacted
    assert redacted == (
        "request used aUtHoRiZaTiOn: bOt "
        "[REDACTED:discord_bot_authorization] and was rejected"
    )


def test_discord_bot_token_assignments_are_redacted() -> None:
    redacted = redact_text(f"boot {FAKE_DISCORD_BOT_TOKEN_ASSIGNMENT} ready")

    assert FAKE_DISCORD_BOT_TOKEN not in redacted
    assert redacted == (
        "boot DISCORD_BOT_TOKEN=[REDACTED:discord_bot_token_assignment] ready"
    )


def test_bare_discord_bot_token_without_context_is_not_redacted() -> None:
    for line in (
        FAKE_DISCORD_BOT_TOKEN,
        f"provider diagnostic value={FAKE_DISCORD_BOT_TOKEN}",
    ):
        assert redact_text(line) == line


def test_ordinary_three_segment_diagnostic_is_not_redacted() -> None:
    for line in (
        "retry route=worker.us-east-1.stable after backoff",
        "bot 1.0.0 started",
        "the Bot v1.2.3 worker is healthy",
    ):
        assert redact_text(line) == line


def test_discord_bot_token_near_matches_are_not_redacted() -> None:
    two_segments = "FAKEFAKEFAKEFAKEFAKE0000." + "FAKE00"
    four_segments = FAKE_DISCORD_BOT_TOKEN + "." + "FAKE_EXTRA-0000"

    for line in (
        two_segments,
        four_segments,
        "Authorization: Bot " + two_segments,
        "Authorization: Bot " + four_segments,
    ):
        assert redact_text(line) == line


def test_discord_bot_assignment_near_matches_use_generic_redaction() -> None:
    two_segments = "FAKEFAKEFAKEFAKEFAKE0000." + "FAKE00"
    four_segments = FAKE_DISCORD_BOT_TOKEN + "." + "FAKE_EXTRA-0000"

    for malformed_value in (two_segments, four_segments):
        redacted = redact_text("DISCORD_BOT_TOKEN=" + malformed_value)

        assert malformed_value not in redacted
        assert redacted == "DISCORD_BOT_TOKEN=[REDACTED:secret_assignment]"
        assert "[REDACTED:discord_bot_token_assignment]" not in redacted


def test_discord_bot_redaction_is_idempotent() -> None:
    for secret in (
        FAKE_DISCORD_BOT_AUTHORIZATION,
        FAKE_DISCORD_BOT_TOKEN_ASSIGNMENT,
    ):
        assert redact_text(redact_text(secret)) == redact_text(secret)


def test_x_api_key_header_value_is_redacted() -> None:
    redacted = redact_text(f"upstream X-API-Key: {FAKE_HEADER_VALUE} rejected")

    assert FAKE_HEADER_VALUE not in redacted
    assert "X-API-Key:" in redacted
    assert "[REDACTED:x_api_key]" in redacted
    assert "upstream " in redacted
    assert " rejected" in redacted


def test_x_api_key_header_is_matched_case_insensitively() -> None:
    redacted = redact_text(f"x-api-key: {FAKE_HEADER_VALUE}")

    assert FAKE_HEADER_VALUE not in redacted
    assert "[REDACTED:x_api_key]" in redacted


def test_agentmail_api_key_prefix_is_redacted() -> None:
    # AgentMail documents the `am_` prefix
    # (https://docs.agentmail.to/knowledge-base/getting-api-key.md).
    redacted = redact_text(f"provider credential {FAKE_AGENTMAIL_API_KEY}")

    assert FAKE_AGENTMAIL_API_KEY not in redacted
    assert "[REDACTED:api_key]" in redacted


def test_short_am_prefix_is_not_treated_as_an_api_key() -> None:
    line = "label am_short stays visible"

    assert redact_text(line) == line


def test_alphanumeric_token_suffix_is_not_an_assignment() -> None:
    line = "mytoken=" + FAKE_EGRESS_SECRET

    assert redact_text(line) == line


def test_benign_near_matches_are_preserved() -> None:
    line = f"session token_count=12 channel=email event_id={BENIGN_EVENT_ID} X-Request-Id: abc"

    assert redact_text(line) == line


def test_installed_filter_redacts_formatted_args() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingLogFilter())
    logger = logging.getLogger("curie.telemetry.redact.args")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        logger.info("ingress X-API-Key: %s", FAKE_CHANNEL_TOKEN)
    finally:
        logger.removeHandler(handler)

    out = stream.getvalue()
    assert FAKE_CHANNEL_TOKEN not in out
    assert "[REDACTED:" in out
    assert "ingress " in out


def test_exception_text_carrying_a_channel_token_assignment_is_redacted() -> None:
    traceback_text = (
        "Traceback (most recent call last):\n"
        '  File "adapter.py", line 1, in handle\n'
        f"RuntimeError: CURIE_CHANNEL_TOKEN={FAKE_CHANNEL_TOKEN}"
    )

    redacted = redact_text(traceback_text)

    assert FAKE_CHANNEL_TOKEN not in redacted
    assert "Traceback (most recent call last):" in redacted
    assert "RuntimeError:" in redacted
    assert "CURIE_CHANNEL_TOKEN=" in redacted
    assert "[REDACTED:channel_token]" in redacted
