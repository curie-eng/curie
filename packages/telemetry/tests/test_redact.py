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
# Shape-valid synthetic bot token: first segment starts with M (a base64
# snowflake id) and is 24 chars, middle is 6, last is 27.
FAKE_SHAPED_DISCORD_BOT_TOKEN = (
    "M" + "FAKEFAKEFAKEFAKEFAKE000." + "FAKE00." + "FAKEFAKEFAKEFAKEFAKEFAKE000"
)
# Regression for the api_key ordering bug: a shape-valid bot token whose HMAC
# segment happens to start with an api_key-style prefix must still be
# redacted as a whole, not partly consumed by the api_key rule first.
FAKE_SHAPED_DISCORD_BOT_TOKEN_AM_HMAC = (
    "M" + "FAKEFAKEFAKEFAKEFAKE000." + "FAKE00." + "am_" + "FAKEFAKEFAKEFAKEFAKEFAKE"
)
FAKE_SHAPED_DISCORD_BOT_TOKEN_SK_HMAC = (
    "M" + "FAKEFAKEFAKEFAKEFAKE000." + "FAKE00." + "FAKEFAKE-sk_" + "FAKEFAKEFAKEFAKE"
)
# Discord executes webhooks at /webhooks/{webhook.id}/{webhook.token}:
# https://docs.discord.com/developers/resources/webhook
FAKE_DISCORD_WEBHOOK_ID = "100000000000000000"
FAKE_DISCORD_WEBHOOK_TOKEN = (
    "FAKE" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE-" + "FAKEFAKEFAKEFAKEFAKEFAKE_FAKE000"
)
FAKE_DISCORD_WEBHOOK_TOKEN_AM = "am_" + "FAKEFAKEFAKEFAKEFAKEFAKEFAKE0000"
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


def test_generic_key_assignments_keep_names_and_remove_values() -> None:
    assignments = {
        "AWS_SECRET_ACCESS_KEY": "FAKE" + "AWSSECRETACCESS0000",
        "MY_PRIVATE_KEY": "FAKE" + "PRIVATEKEYVALUE0000",
    }
    text = "\n".join(f"{key}={value}" for key, value in assignments.items())

    redacted = redact_text(text)

    for key, value in assignments.items():
        assert value not in redacted
        assert f"{key}=[REDACTED:secret_assignment]" in redacted


def test_named_secret_colon_field_keeps_diagnostic_context() -> None:
    value = "FAKE" + "AWSSECRETACCESS0000"
    line = f"step failed AWS_SECRET_ACCESS_KEY: {value} exit code 1"

    redacted = redact_text(line)

    assert redacted == (
        "step failed AWS_SECRET_ACCESS_KEY: [REDACTED:secret_assignment] exit code 1"
    )
    assert value not in redacted
    assert redact_text(redacted) == redacted


def test_named_secret_json_field_keeps_other_fields() -> None:
    value = "FAKE" + "JSONSECRETACCESS0000"
    line = '{"AWS_SECRET_ACCESS_KEY": "' + value + '", "status": "failed"}'

    redacted = redact_text(line)

    assert redacted == (
        '{"AWS_SECRET_ACCESS_KEY": "[REDACTED:secret_assignment]", '
        '"status": "failed"}'
    )
    assert value not in redacted
    assert redact_text(redacted) == redacted


def test_named_secret_dict_field_keeps_other_fields() -> None:
    value = "FAKE" + "DICTSECRETACCESS0000"
    line = "{'AWS_SECRET_ACCESS_KEY': '" + value + "', 'status': 'failed'}"

    redacted = redact_text(line)

    assert redacted == (
        "{'AWS_SECRET_ACCESS_KEY': '[REDACTED:secret_assignment]', "
        "'status': 'failed'}"
    )
    assert value not in redacted
    assert redact_text(redacted) == redacted


def test_credential_assignment_is_redacted() -> None:
    value = "FAKE" + "DATABASECREDENTIAL0000"
    line = f"database rejected DB_CREDENTIAL={value} connection"

    redacted = redact_text(line)

    assert redacted == (
        "database rejected DB_CREDENTIAL=[REDACTED:secret_assignment] connection"
    )
    assert value not in redacted
    assert redact_text(redacted) == redacted


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


def test_three_segment_value_without_discord_shape_or_context_is_not_redacted() -> None:
    # FAKE_DISCORD_BOT_TOKEN's first segment starts with F, so it lacks the
    # bot-token shape (id segment starting M, N or O). With no Authorization or
    # DISCORD_BOT_TOKEN= context either, nothing identifies it as a credential.
    for line in (
        FAKE_DISCORD_BOT_TOKEN,
        f"provider diagnostic value={FAKE_DISCORD_BOT_TOKEN}",
    ):
        assert redact_text(line) == line


def test_bare_shaped_discord_bot_token_is_redacted() -> None:
    assert redact_text(FAKE_SHAPED_DISCORD_BOT_TOKEN) == "[REDACTED:discord_bot_token]"
    assert redact_text(
        f"gateway login with {FAKE_SHAPED_DISCORD_BOT_TOKEN} failed."
    ) == "gateway login with [REDACTED:discord_bot_token] failed."


def test_shaped_discord_bot_token_in_dict_repr_is_redacted() -> None:
    headers = {"Authorization": "Bot " + FAKE_SHAPED_DISCORD_BOT_TOKEN}

    for text in (
        str(headers),
        repr(headers),
        f"HTTPException: 401 Unauthorized request headers={headers!r}",
    ):
        redacted = redact_text(text)

        assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in redacted
        assert "[REDACTED:discord_bot_token]" in redacted
        assert "'Authorization': 'Bot [REDACTED:discord_bot_token]'" in redacted


def test_shaped_discord_bot_token_with_context_keeps_the_context_placeholder() -> None:
    assert redact_text("Authorization: Bot " + FAKE_SHAPED_DISCORD_BOT_TOKEN) == (
        "Authorization: Bot [REDACTED:discord_bot_authorization]"
    )
    assert redact_text("DISCORD_BOT_TOKEN=" + FAKE_SHAPED_DISCORD_BOT_TOKEN) == (
        "DISCORD_BOT_TOKEN=[REDACTED:discord_bot_token_assignment]"
    )


def test_shaped_discord_bot_token_with_am_hmac_prefix_is_fully_redacted() -> None:
    # Before the Discord rules moved ahead of api_key, the "am_" prefix inside
    # the HMAC segment let api_key consume part of the token first, leaving
    # the rest of the shape (and part of the token) unredacted.
    redacted = redact_text(
        f"gateway login with {FAKE_SHAPED_DISCORD_BOT_TOKEN_AM_HMAC} failed."
    )

    assert FAKE_SHAPED_DISCORD_BOT_TOKEN_AM_HMAC not in redacted
    assert redacted == "gateway login with [REDACTED:discord_bot_token] failed."


def test_shaped_discord_bot_token_with_sk_hmac_infix_is_fully_redacted() -> None:
    redacted = redact_text(
        f"gateway login with {FAKE_SHAPED_DISCORD_BOT_TOKEN_SK_HMAC} failed."
    )

    assert FAKE_SHAPED_DISCORD_BOT_TOKEN_SK_HMAC not in redacted
    assert redacted == "gateway login with [REDACTED:discord_bot_token] failed."


def test_discord_webhook_token_with_am_prefix_is_fully_redacted() -> None:
    prefix = "https://discord.com/api/webhooks/" + FAKE_DISCORD_WEBHOOK_ID + "/"
    url = prefix + FAKE_DISCORD_WEBHOOK_TOKEN_AM

    redacted = redact_text(f"POST {url} returned 404")

    assert FAKE_DISCORD_WEBHOOK_TOKEN_AM not in redacted
    assert redacted == f"POST {prefix}[REDACTED:discord_webhook_url] returned 404"


def test_discord_bot_token_first_segment_boundary_is_redacted() -> None:
    middle = "FAKE00"
    last = ("FAKE" * 7)[:27]  # valid minimum last-segment length
    for total_len in (24, 28):  # M + 23, M + 27
        first = "M" + ("FAKE" * 8)[: total_len - 1]
        assert len(first) == total_len
        token = first + "." + middle + "." + last

        assert redact_text(token) == "[REDACTED:discord_bot_token]"


def test_discord_bot_token_first_segment_out_of_range_is_not_redacted() -> None:
    middle = "FAKE00"
    last = ("FAKE" * 7)[:27]
    for total_len in (23, 29):  # one below min (23), one above max (28)
        first = "M" + ("FAKE" * 8)[: total_len - 1]
        assert len(first) == total_len
        token = first + "." + middle + "." + last

        assert redact_text(token) == token


def test_discord_bot_token_middle_segment_wrong_length_is_not_redacted() -> None:
    first = "M" + ("FAKE" * 6)[:23]
    last = ("FAKE" * 7)[:27]
    for middle_len in (5, 7):
        middle = ("FAKE" * 2)[:middle_len]
        token = first + "." + middle + "." + last

        assert redact_text(token) == token


def test_discord_bot_token_last_segment_boundary_is_redacted() -> None:
    first = "M" + ("FAKE" * 6)[:23]
    middle = "FAKE00"
    for total_len in (27, 38):
        last = ("FAKE" * 10)[:total_len]
        assert len(last) == total_len
        token = first + "." + middle + "." + last

        assert redact_text(token) == "[REDACTED:discord_bot_token]"


def test_discord_bot_token_last_segment_too_short_is_not_redacted() -> None:
    first = "M" + ("FAKE" * 6)[:23]
    middle = "FAKE00"
    last = ("FAKE" * 7)[:26]
    token = first + "." + middle + "." + last

    assert redact_text(token) == token


def test_discord_bot_token_last_segment_too_long_fails_trailing_lookahead() -> None:
    # The quantifier is greedy and takes at most 38 chars, then the trailing
    # negative lookahead ``(?![A-Za-z0-9_-]|\.[A-Za-z0-9_-])`` requires the
    # character right after the match to not continue the same charset. With
    # a 39-char last segment made of one uniform charset, every possible
    # match length from 27 to 38 is immediately followed by one more
    # charset character, so the lookahead fails at every backtrack position
    # and the token is left untouched.
    first = "M" + ("FAKE" * 6)[:23]
    middle = "FAKE00"
    last = ("FAKE" * 10)[:39]
    token = first + "." + middle + "." + last

    assert redact_text(token) == token


def test_discord_bot_token_shape_near_matches_are_not_redacted() -> None:
    first, middle, last = FAKE_SHAPED_DISCORD_BOT_TOKEN.split(".")
    for line in (
        # First segment must start with M, N or O.
        "P" + first[1:] + "." + middle + "." + last,
        # Middle segment must be exactly six characters.
        first + "." + middle + "0" + "." + last,
        first + "." + middle[:-1] + "." + last,
        # First segment too short, last segment too short or too long.
        first[:-1] + "." + middle + "." + last,
        first + "." + middle + "." + last[:-1],
        first + "." + middle + "." + last + "FAKEFAKEFAKE",
        # Embedded in a longer run, or part of a four-segment value.
        "FAKE" + FAKE_SHAPED_DISCORD_BOT_TOKEN,
        "FAKE-" + FAKE_SHAPED_DISCORD_BOT_TOKEN,
        FAKE_SHAPED_DISCORD_BOT_TOKEN + "." + "FAKE00",
        "FAKE00" + "." + FAKE_SHAPED_DISCORD_BOT_TOKEN,
    ):
        assert redact_text(line) == line


def test_discord_webhook_url_token_is_redacted_and_url_stays_diagnostic() -> None:
    path = "/webhooks/" + FAKE_DISCORD_WEBHOOK_ID + "/"
    for prefix, suffix in (
        ("https://discord.com/api" + path, ""),
        ("https://discordapp.com/api" + path, ""),
        ("https://canary.discord.com/api/v10" + path, ""),
        ("https://discord.com/api" + path, "?wait=true"),
    ):
        url = prefix + FAKE_DISCORD_WEBHOOK_TOKEN + suffix

        redacted = redact_text(f"POST {url} returned 404")

        assert FAKE_DISCORD_WEBHOOK_TOKEN not in redacted
        assert redacted == (
            f"POST {prefix}[REDACTED:discord_webhook_url]{suffix} returned 404"
        )


def test_non_discord_webhook_url_is_not_redacted() -> None:
    for line in (
        "https://example.com/api/webhooks/" + FAKE_DISCORD_WEBHOOK_ID + "/FAKE00",
        "https://discord.com/api/webhooks/FAKE00/FAKE00",
    ):
        assert redact_text(line) == line


def test_discord_shape_redaction_is_idempotent() -> None:
    webhook = (
        "https://discord.com/api/webhooks/"
        + FAKE_DISCORD_WEBHOOK_ID
        + "/"
        + FAKE_DISCORD_WEBHOOK_TOKEN
    )
    for secret in (FAKE_SHAPED_DISCORD_BOT_TOKEN, webhook):
        assert redact_text(redact_text(secret)) == redact_text(secret)


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


# Regression for the header-ordering bug: a Discord-shaped token embedded in a
# header value, followed by trailing non-token text, must be redacted whole by
# the whole-value header rule (bearer_token / x_api_key), which now runs
# before the Discord rules. Previously the Discord rule ran first, replaced
# only the token portion with a placeholder, and the placeholder's
# ``(?!\[REDACTED:)`` guard then blocked the header rule from matching the
# rest of the value -- leaking the trailing suffix.
FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX = FAKE_SHAPED_DISCORD_BOT_TOKEN + "/FAKE_SUFFIX"


def test_bearer_header_with_discord_shaped_token_and_suffix_is_fully_redacted() -> None:
    redacted = redact_text(f"Bearer {FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}")

    assert redacted == "[REDACTED:bearer_token]"
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in redacted
    assert "FAKE_SUFFIX" not in redacted


def test_x_api_key_header_with_discord_shaped_token_and_suffix_is_fully_redacted() -> None:
    redacted = redact_text(f"X-API-Key: {FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}")

    assert redacted == "X-API-Key: [REDACTED:x_api_key]"
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in redacted
    assert "FAKE_SUFFIX" not in redacted


def test_bearer_header_with_discord_shaped_token_and_suffix_is_redacted_through_filter() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingLogFilter())
    logger = logging.getLogger("curie.telemetry.redact.discord_bearer_suffix")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        logger.info("upstream Bearer %s rejected", FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX)
    finally:
        logger.removeHandler(handler)

    out = stream.getvalue()
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in out
    assert "FAKE_SUFFIX" not in out
    assert "[REDACTED:bearer_token]" in out


def test_x_api_key_header_with_discord_shaped_token_and_suffix_is_redacted_through_filter() -> (
    None
):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingLogFilter())
    logger = logging.getLogger("curie.telemetry.redact.discord_x_api_key_suffix")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        logger.info("upstream X-API-Key: %s rejected", FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX)
    finally:
        logger.removeHandler(handler)

    out = stream.getvalue()
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in out
    assert "FAKE_SUFFIX" not in out
    assert "[REDACTED:x_api_key]" in out


# Regression for the ruling above: secret_assignment now runs before the
# Discord shape rule, so a ``token=``/``password=`` value carrying a
# Discord-shaped token plus a trailing suffix is redacted whole rather than
# leaving the suffix exposed after a narrower placeholder.
def test_token_assignment_with_discord_shaped_token_and_suffix_is_fully_redacted() -> None:
    redacted = redact_text(f"token={FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}")

    assert redacted == "token=[REDACTED:secret_assignment]"
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in redacted
    assert "FAKE_SUFFIX" not in redacted


def test_password_assignment_with_discord_shaped_token_and_suffix_is_fully_redacted() -> None:
    redacted = redact_text(f"password={FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}")

    assert redacted == "password=[REDACTED:secret_assignment]"
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in redacted
    assert "FAKE_SUFFIX" not in redacted


def test_token_assignment_with_discord_shaped_token_and_suffix_is_redacted_through_filter() -> (
    None
):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingLogFilter())
    logger = logging.getLogger("curie.telemetry.redact.discord_token_assignment_suffix")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        logger.info("upstream token=%s rejected", FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX)
    finally:
        logger.removeHandler(handler)

    out = stream.getvalue()
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in out
    assert "FAKE_SUFFIX" not in out
    assert "[REDACTED:secret_assignment]" in out


def test_password_assignment_with_discord_shaped_token_and_suffix_is_redacted_through_filter() -> (
    None
):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingLogFilter())
    logger = logging.getLogger("curie.telemetry.redact.discord_password_assignment_suffix")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        logger.info("upstream password=%s rejected", FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX)
    finally:
        logger.removeHandler(handler)

    out = stream.getvalue()
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in out
    assert "FAKE_SUFFIX" not in out
    assert "[REDACTED:secret_assignment]" in out


def test_discord_bot_token_assignment_with_trailing_suffix_falls_through_to_secret_assignment() -> (
    None
):
    # The lookahead narrowing (step 2 of the ruling): a value followed by any
    # non-space suffix is no longer eligible for the named Discord placeholder
    # and instead falls through whole to secret_assignment.
    redacted = redact_text("DISCORD_BOT_TOKEN=" + FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX)

    assert redacted == "DISCORD_BOT_TOKEN=[REDACTED:secret_assignment]"
    assert FAKE_SHAPED_DISCORD_BOT_TOKEN not in redacted
    assert "FAKE_SUFFIX" not in redacted
    assert "[REDACTED:discord_bot_token_assignment]" not in redacted


def test_bare_discord_bot_token_assignment_still_uses_named_placeholder() -> None:
    # No trailing suffix: the value ends at end-of-string, so the narrowed
    # ``(?!\S)`` lookahead still matches and the named placeholder holds.
    redacted = redact_text("DISCORD_BOT_TOKEN=" + FAKE_SHAPED_DISCORD_BOT_TOKEN)

    assert redacted == "DISCORD_BOT_TOKEN=[REDACTED:discord_bot_token_assignment]"


# Channel token whose signature segment happens to contain an api_key-style
# ``am_`` prefix, mirroring the finding's exact shape.
FAKE_CHANNEL_TOKEN_AM_SIG = (
    "chn." + "ZXhhbXBsZWNoYW5uZWxwYXlsb2Fk." + "am_" + "A" * 24
)


def test_channel_token_with_am_signature_and_suffix_falls_through_to_secret_assignment() -> (
    None
):
    # channel_token consumes only up to the signature (it cannot cross the
    # ``/``), so its placeholder is left with a trailing suffix. The widened
    # secret_assignment guard must still consume the whole value.
    redacted = redact_text("token=" + FAKE_CHANNEL_TOKEN_AM_SIG + "/FAKE_SUFFIX")

    assert redacted == "token=[REDACTED:secret_assignment]"
    assert "FAKE_SUFFIX" not in redacted
    assert "[REDACTED:channel_token]" not in redacted


def test_channel_token_assignment_with_suffix_falls_through_to_secret_assignment() -> None:
    redacted = redact_text("token=" + FAKE_CHANNEL_TOKEN + "/FAKE_SUFFIX")

    assert redacted == "token=[REDACTED:secret_assignment]"
    assert "FAKE_SUFFIX" not in redacted
    assert "[REDACTED:channel_token]" not in redacted


def test_x_api_key_header_with_channel_token_and_suffix_falls_through_to_x_api_key() -> None:
    redacted = redact_text("X-API-Key: " + FAKE_CHANNEL_TOKEN + "/FAKE_SUFFIX")

    assert redacted == "X-API-Key: [REDACTED:x_api_key]"
    assert "FAKE_SUFFIX" not in redacted
    assert "[REDACTED:channel_token]" not in redacted


def test_secret_assignment_placeholder_is_idempotent() -> None:
    once = "token=[REDACTED:secret_assignment]"

    assert redact_text(once) == once


def test_channel_token_assignment_is_idempotent() -> None:
    once = redact_text("CURIE_CHANNEL_TOKEN=" + FAKE_CHANNEL_TOKEN)
    twice = redact_text(once)

    assert once == "CURIE_CHANNEL_TOKEN=[REDACTED:channel_token]"
    assert twice == once
