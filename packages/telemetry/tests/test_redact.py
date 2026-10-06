"""Redaction closes authentication formats before any telemetry export."""

from __future__ import annotations

import io
import logging

import pytest
from curie_telemetry.redact import RedactingLogFilter, redact_text

# Hoisted synthetic values. The check-secrets hook false-positives on inline
# token literals, so prefixes are split rather than written at the call site.
#
# Channel tokens are minted as ``chn.{payload}.{signature}``
# (``apps/api/src/curie_api/channel_token.py``, prefix ``chn``). The issue's
# ``chn-{channel_id}-{digest}`` shape is ``_event_id``, not a credential.
FAKE_CHANNEL_TOKEN = "chn." + "ZXhhbXBsZWNoYW5uZWxwYXlsb2Fk." + "FAKEFAKEFAKESIG0000"
# Shapes from curie_api.sandbox_token.mint and curie_worker.caller_token.mint.
FAKE_SANDBOX_TOKEN = "sbx." + "ZXhhbXBsZXNhbmRib3hwYXlsb2Fk." + "FAKEFAKEFAKESIG0000"
FAKE_CONNECTOR_CALLER_TOKEN = "cct." + "ZXhhbXBsZWNhbGxlcnBheWxvYWQ." + "FAKEFAKEFAKESIG0000"
# Provider docs: "API keys start with `am_`"
# https://docs.agentmail.to/knowledge-base/getting-api-key.md
FAKE_AGENTMAIL_API_KEY = "am_" + "FAKEFAKEFAKEFAKEFAKE0000"
FAKE_EGRESS_SECRET = "FAKEFAKEFAKEEGRESS0000"
FAKE_HEADER_VALUE = "FAKEFAKEFAKEHEADERVALUE0000"
# Discord documents ``Authorization: Bot id.timestamp.hmac``:
# https://docs.discord.com/developers/reference
# These synthetic segments mirror its example's 24/6/27 lengths only to keep
# the positive representative; the lengths are not part of the asserted grammar.
FAKE_DISCORD_BOT_TOKEN = "FAKEFAKEFAKEFAKEFAKE0000." + "FAKE00." + "FAKEFAKEFAKEFAKEFAKEFAKE000"
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


# A Discord-shaped token embedded in a header or assignment value, followed
# by trailing non-token text, must be redacted whole by the whole-value rule
# (bearer_token / x_api_key / secret_assignment), which runs before the
# Discord rules; otherwise the Discord placeholder's ``(?!\[REDACTED:)``
# guard blocks the whole-value rule and leaks the suffix.
FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX = FAKE_SHAPED_DISCORD_BOT_TOKEN + "/FAKE_SUFFIX"
# Channel token whose signature segment happens to contain an api_key-style
# ``am_`` prefix.
FAKE_CHANNEL_TOKEN_AM_SIG = "chn." + "ZXhhbXBsZWNoYW5uZWxwYXlsb2Fk." + "am_" + "A" * 24
FAKE_BASIC_AUTH = "ZmFrZS11c2Vy" + "OmZha2UtcGFzc3dvcmQ="
FAKE_SLACK_APP_TOKEN = "xapp-0-0000000000-0000000000-FAKEFAKEFAKEFAKE"
FAKE_AWS_SECRET = "FAKE" + "AWSSECRETACCESS0000"
FAKE_WEBHOOK_PATH = "/webhooks/" + FAKE_DISCORD_WEBHOOK_ID + "/"
FAKE_DISCORD_WEBHOOK_URL = (
    "https://discord.com/api" + FAKE_WEBHOOK_PATH + FAKE_DISCORD_WEBHOOK_TOKEN
)
FAKE_DISCORD_HEADERS = {"Authorization": "Bot " + FAKE_SHAPED_DISCORD_BOT_TOKEN}
REDACTED_DISCORD_HEADERS = "{'Authorization': 'Bot [REDACTED:discord_bot_token]'}"
TWO_SEGMENTS = "FAKEFAKEFAKEFAKEFAKE0000." + "FAKE00"
FOUR_SEGMENTS = FAKE_DISCORD_BOT_TOKEN + "." + "FAKE_EXTRA-0000"
_FIRST, _MIDDLE, _LAST = FAKE_SHAPED_DISCORD_BOT_TOKEN.split(".")
# Presigned object URLs carry their credential in prefixed query parameters:
# AWS SigV4 (``X-Amz-Credential``, ``X-Amz-Security-Token``, ``X-Amz-Signature``,
# https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html)
# and GCS V4 (``X-Goog-Credential``, ``X-Goog-Signature``,
# https://cloud.google.com/storage/docs/access-control/signed-urls). The
# algorithm, date, expiry and signed-header parameters are not secret and stay
# visible for diagnosis.
FAKE_AMZ_CREDENTIAL = "ASIA" + "FAKEFAKEFAKE0000%2F20261005%2Fus-east-1%2Fs3%2Faws4_request"
FAKE_AMZ_SECURITY_TOKEN = "FAKE" + "SESSIONTOKEN0000%2B%2Ffake%3D%3D"
FAKE_PRESIGN_SIGNATURE = "fake" + "0000000000000000000000000000000000000000000000000000000000"
_AMZ_PUBLIC_PARAMS = (
    "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20261005T000000Z"
    "&X-Amz-Expires=300&X-Amz-SignedHeaders=host"
)
_GOOG_PUBLIC_PARAMS = (
    "X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Date=20261005T000000Z"
    "&X-Goog-Expires=300&X-Goog-SignedHeaders=host"
)
FAKE_S3_PRESIGNED_URL = (
    "https://bucket.example.com/attachments/report.pdf?"
    + _AMZ_PUBLIC_PARAMS
    + "&X-Amz-Credential="
    + FAKE_AMZ_CREDENTIAL
    + "&X-Amz-Security-Token="
    + FAKE_AMZ_SECURITY_TOKEN
    + "&X-Amz-Signature="
    + FAKE_PRESIGN_SIGNATURE
)
REDACTED_S3_PRESIGNED_URL = (
    "https://bucket.example.com/attachments/report.pdf?"
    + _AMZ_PUBLIC_PARAMS
    + "[REDACTED:url_secret_param]" * 3
)
FAKE_GCS_PRESIGNED_URL = (
    "https://storage.example.com/bucket/report.pdf?"
    + _GOOG_PUBLIC_PARAMS
    + "&X-Goog-Credential=signer%40example.com%2F20261005%2Fauto%2Fstorage%2Fgoog4_request"
    + "&X-Goog-Signature="
    + FAKE_PRESIGN_SIGNATURE
)
REDACTED_GCS_PRESIGNED_URL = (
    "https://storage.example.com/bucket/report.pdf?"
    + _GOOG_PUBLIC_PARAMS
    + "[REDACTED:url_secret_param]" * 2
)


def _bot_token(first_len: int = 24, middle_len: int = 6, last_len: int = 27) -> str:
    """A Discord bot-token shape with the given segment lengths, first starting M."""
    return (
        "M"
        + ("FAKE" * 8)[: first_len - 1]
        + "."
        + ("FAKE" * 2)[:middle_len]
        + "."
        + ("FAKE" * 10)[:last_len]
    )


# Each case: input text, exact redacted output. Every output must also be a
# fixed point of redaction (idempotence).
REDACTED_CASES = [
    pytest.param(
        f"attachment fetch failed for {FAKE_S3_PRESIGNED_URL} (403 Forbidden)",
        f"attachment fetch failed for {REDACTED_S3_PRESIGNED_URL} (403 Forbidden)",
        id="s3_presigned_url_signature_credential_and_session_token_are_redacted",
    ),
    pytest.param(
        FAKE_GCS_PRESIGNED_URL,
        REDACTED_GCS_PRESIGNED_URL,
        id="gcs_presigned_url_signature_and_credential_are_redacted",
    ),
    *(
        pytest.param(
            f"https://bucket.example.com/key?{name}={value}",
            "https://bucket.example.com/key[REDACTED:url_secret_param]",
            id=f"presigned_param_is_redacted_in_any_case-{name}",
        )
        for name, value in (
            ("X-Amz-Signature", FAKE_PRESIGN_SIGNATURE),
            ("x-amz-credential", FAKE_AMZ_CREDENTIAL),
            ("X-AMZ-SECURITY-TOKEN", FAKE_AMZ_SECURITY_TOKEN),
            ("x-goog-signature", FAKE_PRESIGN_SIGNATURE),
            ("X-Goog-Credential", "signer%40example.com%2F20261005"),
        )
    ),
    *(
        pytest.param(
            f"before ({token}) after",
            f"before ([REDACTED:{rule}]) after",
            id=f"{rule}_bare_value_preserves_punctuation",
        )
        for rule, token in (
            ("sandbox_token", FAKE_SANDBOX_TOKEN),
            ("connector_caller_token", FAKE_CONNECTOR_CALLER_TOKEN),
        )
    ),
    *(
        pytest.param(
            repr({"credential": token, "status": "ok"}),
            f"{{'credential': '[REDACTED:{rule}]', 'status': 'ok'}}",
            id=f"{rule}_dictionary_value_keeps_harmless_fields",
        )
        for rule, token in (
            ("sandbox_token", FAKE_SANDBOX_TOKEN),
            ("connector_caller_token", FAKE_CONNECTOR_CALLER_TOKEN),
        )
    ),
    *(
        pytest.param(
            f"CURIE_RUNNER_TOKEN={token}",
            f"CURIE_RUNNER_TOKEN=[REDACTED:{rule}]",
            id=f"{rule}_assignment_uses_named_placeholder",
        )
        for rule, token in (
            ("sandbox_token", FAKE_SANDBOX_TOKEN),
            ("connector_caller_token", FAKE_CONNECTOR_CALLER_TOKEN),
        )
    ),
    *(
        pytest.param(
            f"{prefix}{token}/FAKE_SUFFIX",
            f"{retained}[REDACTED:{context_rule}]",
            id=f"{rule}_{context_rule}_overlap_consumes_the_entire_value",
        )
        for rule, token in (
            ("sandbox_token", FAKE_SANDBOX_TOKEN),
            ("connector_caller_token", FAKE_CONNECTOR_CALLER_TOKEN),
        )
        for prefix, retained, context_rule in (
            ("Authorization: Bearer ", "Authorization: ", "bearer_token"),
            ("X-API-Key: ", "X-API-Key: ", "x_api_key"),
            ("token=", "token=", "secret_assignment"),
        )
    ),
    pytest.param(
        f"Authorization: Basic {FAKE_BASIC_AUTH}",
        "Authorization: [REDACTED:basic_auth]",
        id="basic_authorization_value_is_removed",
    ),
    pytest.param(
        "connection failed postgresql://fake-user:fake-password@db.example.com:5432/acme",
        "connection failed postgresql://[REDACTED:dsn_userinfo]@db.example.com:5432/acme",
        id="dsn_userinfo_is_removed_while_host_and_database_remain_diagnostic",
    ),
    pytest.param(
        f"app credential {FAKE_SLACK_APP_TOKEN}",
        "app credential [REDACTED:slack_token]",
        id="slack_app_level_token_is_removed",
    ),
    pytest.param(
        f"adapter credential {FAKE_CHANNEL_TOKEN}",
        "adapter credential [REDACTED:channel_token]",
        id="bare_channel_token_shape_is_redacted",
    ),
    pytest.param(
        f"boot CURIE_CHANNEL_TOKEN={FAKE_HEADER_VALUE} ready",
        "boot CURIE_CHANNEL_TOKEN=[REDACTED:secret_assignment] ready",
        id="curie_channel_token_assignment_is_redacted",
    ),
    pytest.param(
        f"boot CURIE_EGRESS_SECRET={FAKE_EGRESS_SECRET} ready",
        "boot CURIE_EGRESS_SECRET=[REDACTED:secret_assignment] ready",
        id="curie_egress_secret_assignment_is_redacted",
    ),
    pytest.param(
        f"AWS_SECRET_ACCESS_KEY={FAKE_AWS_SECRET}\nMY_PRIVATE_KEY=FAKE" + "PRIVATEKEYVALUE0000",
        "AWS_SECRET_ACCESS_KEY=[REDACTED:secret_assignment]\n"
        "MY_PRIVATE_KEY=[REDACTED:secret_assignment]",
        id="generic_key_assignments_keep_names_and_remove_values",
    ),
    pytest.param(
        f"step failed AWS_SECRET_ACCESS_KEY: {FAKE_AWS_SECRET} exit code 1",
        "step failed AWS_SECRET_ACCESS_KEY: [REDACTED:secret_assignment] exit code 1",
        id="named_secret_colon_field_keeps_diagnostic_context",
    ),
    pytest.param(
        '{"AWS_SECRET_ACCESS_KEY": "FAKE' + 'JSONSECRETACCESS0000", "status": "failed"}',
        '{"AWS_SECRET_ACCESS_KEY": "[REDACTED:secret_assignment]", "status": "failed"}',
        id="named_secret_json_field_keeps_other_fields",
    ),
    pytest.param(
        "{'AWS_SECRET_ACCESS_KEY': 'FAKE" + "DICTSECRETACCESS0000', 'status': 'failed'}",
        "{'AWS_SECRET_ACCESS_KEY': '[REDACTED:secret_assignment]', 'status': 'failed'}",
        id="named_secret_dict_field_keeps_other_fields",
    ),
    pytest.param(
        "database rejected DB_CREDENTIAL=FAKE" + "DATABASECREDENTIAL0000 connection",
        "database rejected DB_CREDENTIAL=[REDACTED:secret_assignment] connection",
        id="credential_assignment_is_redacted",
    ),
    pytest.param(
        f"request used {FAKE_DISCORD_BOT_AUTHORIZATION} and was rejected",
        "request used Authorization: Bot [REDACTED:discord_bot_authorization] and was rejected",
        id="discord_bot_authorization_is_redacted",
    ),
    pytest.param(
        f"request used aUtHoRiZaTiOn: bOt {FAKE_DISCORD_BOT_TOKEN} and was rejected",
        "request used aUtHoRiZaTiOn: bOt [REDACTED:discord_bot_authorization] and was rejected",
        id="discord_bot_authorization_is_matched_case_insensitively",
    ),
    pytest.param(
        f"boot {FAKE_DISCORD_BOT_TOKEN_ASSIGNMENT} ready",
        "boot DISCORD_BOT_TOKEN=[REDACTED:discord_bot_token_assignment] ready",
        id="discord_bot_token_assignments_are_redacted",
    ),
    pytest.param(
        FAKE_SHAPED_DISCORD_BOT_TOKEN,
        "[REDACTED:discord_bot_token]",
        id="bare_shaped_discord_bot_token_is_redacted-bare",
    ),
    pytest.param(
        f"gateway login with {FAKE_SHAPED_DISCORD_BOT_TOKEN} failed.",
        "gateway login with [REDACTED:discord_bot_token] failed.",
        id="bare_shaped_discord_bot_token_is_redacted-in_sentence",
    ),
    pytest.param(
        str(FAKE_DISCORD_HEADERS),
        REDACTED_DISCORD_HEADERS,
        id="shaped_discord_bot_token_in_dict_repr_is_redacted-repr",
    ),
    pytest.param(
        f"HTTPException: 401 Unauthorized request headers={FAKE_DISCORD_HEADERS!r}",
        f"HTTPException: 401 Unauthorized request headers={REDACTED_DISCORD_HEADERS}",
        id="shaped_discord_bot_token_in_dict_repr_is_redacted-exception",
    ),
    pytest.param(
        "Authorization: Bot " + FAKE_SHAPED_DISCORD_BOT_TOKEN,
        "Authorization: Bot [REDACTED:discord_bot_authorization]",
        id="shaped_discord_bot_token_with_context_keeps_the_context_placeholder-header",
    ),
    pytest.param(
        "DISCORD_BOT_TOKEN=" + FAKE_SHAPED_DISCORD_BOT_TOKEN,
        "DISCORD_BOT_TOKEN=[REDACTED:discord_bot_token_assignment]",
        id="shaped_discord_bot_token_with_context_keeps_the_context_placeholder-assignment",
    ),
    # Before the Discord rules moved ahead of api_key, the "am_" prefix inside
    # the HMAC segment let api_key consume part of the token first.
    pytest.param(
        f"gateway login with {FAKE_SHAPED_DISCORD_BOT_TOKEN_AM_HMAC} failed.",
        "gateway login with [REDACTED:discord_bot_token] failed.",
        id="shaped_discord_bot_token_with_am_hmac_prefix_is_fully_redacted",
    ),
    pytest.param(
        f"gateway login with {FAKE_SHAPED_DISCORD_BOT_TOKEN_SK_HMAC} failed.",
        "gateway login with [REDACTED:discord_bot_token] failed.",
        id="shaped_discord_bot_token_with_sk_hmac_infix_is_fully_redacted",
    ),
    pytest.param(
        "POST https://discord.com/api"
        + FAKE_WEBHOOK_PATH
        + FAKE_DISCORD_WEBHOOK_TOKEN_AM
        + " returned 404",
        "POST https://discord.com/api"
        + FAKE_WEBHOOK_PATH
        + "[REDACTED:discord_webhook_url] returned 404",
        id="discord_webhook_token_with_am_prefix_is_fully_redacted",
    ),
    *(
        pytest.param(
            _bot_token(first_len=n),
            "[REDACTED:discord_bot_token]",
            id=f"discord_bot_token_first_segment_boundary_is_redacted-{n}",
        )
        for n in (24, 28)
    ),
    *(
        pytest.param(
            _bot_token(last_len=n),
            "[REDACTED:discord_bot_token]",
            id=f"discord_bot_token_last_segment_boundary_is_redacted-{n}",
        )
        for n in (27, 38)
    ),
    *(
        pytest.param(
            f"POST {prefix}{FAKE_DISCORD_WEBHOOK_TOKEN}{suffix} returned 404",
            f"POST {prefix}[REDACTED:discord_webhook_url]{suffix} returned 404",
            id=f"discord_webhook_url_token_is_redacted_and_url_stays_diagnostic-{variant}",
        )
        for variant, prefix, suffix in (
            ("discord", "https://discord.com/api" + FAKE_WEBHOOK_PATH, ""),
            ("discordapp", "https://discordapp.com/api" + FAKE_WEBHOOK_PATH, ""),
            ("canary_v10", "https://canary.discord.com/api/v10" + FAKE_WEBHOOK_PATH, ""),
            ("query", "https://discord.com/api" + FAKE_WEBHOOK_PATH, "?wait=true"),
        )
    ),
    *(
        pytest.param(
            "DISCORD_BOT_TOKEN=" + value,
            "DISCORD_BOT_TOKEN=[REDACTED:secret_assignment]",
            id=f"discord_bot_assignment_near_matches_use_generic_redaction-{variant}",
        )
        for variant, value in (("two_segments", TWO_SEGMENTS), ("four_segments", FOUR_SEGMENTS))
    ),
    pytest.param(
        f"upstream X-API-Key: {FAKE_HEADER_VALUE} rejected",
        "upstream X-API-Key: [REDACTED:x_api_key] rejected",
        id="x_api_key_header_value_is_redacted",
    ),
    pytest.param(
        f"x-api-key: {FAKE_HEADER_VALUE}",
        "x-api-key: [REDACTED:x_api_key]",
        id="x_api_key_header_is_matched_case_insensitively",
    ),
    pytest.param(
        f"provider credential {FAKE_AGENTMAIL_API_KEY}",
        "provider credential [REDACTED:api_key]",
        id="agentmail_api_key_prefix_is_redacted",
    ),
    pytest.param(
        "Traceback (most recent call last):\n"
        '  File "adapter.py", line 1, in handle\n'
        f"RuntimeError: CURIE_CHANNEL_TOKEN={FAKE_CHANNEL_TOKEN}",
        "Traceback (most recent call last):\n"
        '  File "adapter.py", line 1, in handle\n'
        "RuntimeError: CURIE_CHANNEL_TOKEN=[REDACTED:channel_token]",
        id="exception_text_carrying_a_channel_token_assignment_is_redacted",
    ),
    pytest.param(
        f"Bearer {FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}",
        "[REDACTED:bearer_token]",
        id="bearer_header_with_discord_shaped_token_and_suffix_is_fully_redacted",
    ),
    pytest.param(
        f"X-API-Key: {FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}",
        "X-API-Key: [REDACTED:x_api_key]",
        id="x_api_key_header_with_discord_shaped_token_and_suffix_is_fully_redacted",
    ),
    pytest.param(
        f"token={FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}",
        "token=[REDACTED:secret_assignment]",
        id="token_assignment_with_discord_shaped_token_and_suffix_is_fully_redacted",
    ),
    pytest.param(
        f"password={FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX}",
        "password=[REDACTED:secret_assignment]",
        id="password_assignment_with_discord_shaped_token_and_suffix_is_fully_redacted",
    ),
    # A value followed by any non-space suffix is not eligible for the named
    # Discord placeholder and falls through whole to secret_assignment.
    pytest.param(
        "DISCORD_BOT_TOKEN=" + FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX,
        "DISCORD_BOT_TOKEN=[REDACTED:secret_assignment]",
        id="discord_bot_token_assignment_with_trailing_suffix_falls_through_to_secret_assignment",
    ),
    # No trailing suffix: the ``(?!\S)`` lookahead still matches at end of string.
    pytest.param(
        "DISCORD_BOT_TOKEN=" + FAKE_SHAPED_DISCORD_BOT_TOKEN,
        "DISCORD_BOT_TOKEN=[REDACTED:discord_bot_token_assignment]",
        id="bare_discord_bot_token_assignment_still_uses_named_placeholder",
    ),
    # channel_token cannot cross the ``/``, so its placeholder would be left
    # with a trailing suffix; secret_assignment must consume the whole value.
    pytest.param(
        "token=" + FAKE_CHANNEL_TOKEN_AM_SIG + "/FAKE_SUFFIX",
        "token=[REDACTED:secret_assignment]",
        id="channel_token_with_am_signature_and_suffix_falls_through_to_secret_assignment",
    ),
    pytest.param(
        "token=" + FAKE_CHANNEL_TOKEN + "/FAKE_SUFFIX",
        "token=[REDACTED:secret_assignment]",
        id="channel_token_assignment_with_suffix_falls_through_to_secret_assignment",
    ),
    pytest.param(
        "X-API-Key: " + FAKE_CHANNEL_TOKEN + "/FAKE_SUFFIX",
        "X-API-Key: [REDACTED:x_api_key]",
        id="x_api_key_header_with_channel_token_and_suffix_falls_through_to_x_api_key",
    ),
    pytest.param(
        "CURIE_CHANNEL_TOKEN=" + FAKE_CHANNEL_TOKEN,
        "CURIE_CHANNEL_TOKEN=[REDACTED:channel_token]",
        id="channel_token_assignment_is_idempotent",
    ),
]


@pytest.mark.parametrize(("text", "expected"), REDACTED_CASES)
def test_redaction(text: str, expected: str) -> None:
    assert redact_text(text) == expected
    assert redact_text(expected) == expected


# Each case: text that carries no credential and must pass through unchanged.
PRESERVED_CASES = [
    pytest.param(
        "https://bucket.example.com/attachments/report.pdf?" + _AMZ_PUBLIC_PARAMS,
        id="presigned_url_public_amz_params_are_preserved",
    ),
    pytest.param(
        "https://storage.example.com/bucket/report.pdf?" + _GOOG_PUBLIC_PARAMS,
        id="presigned_url_public_goog_params_are_preserved",
    ),
    *(
        pytest.param(text, id=f"curie_token_near_match_is_preserved_{index}")
        for index, text in enumerate(
            (
                "sbx.payload",
                "cct.payload",
                "sbx..signature",
                "cct.payload.",
                "sandbox=sbx-acme-example caller=cct-acme-example",
            )
        )
    ),
    pytest.param(
        f"inbound admitted event_id={BENIGN_EVENT_ID}",
        id="channel_event_id_is_not_treated_as_a_credential",
    ),
    # CURIE_EGRESS_SECRET has no unique prefix. A regex that claimed to
    # recognize the value itself would also redact ordinary diagnostic text.
    pytest.param(
        f"retry after {FAKE_EGRESS_SECRET} milliseconds",
        id="unprefixed_secret_without_assignment_or_header_context_is_not_redacted",
    ),
    # FAKE_DISCORD_BOT_TOKEN's first segment starts with F, so it lacks the
    # bot-token shape (id segment starting M, N or O) and has no context.
    pytest.param(
        FAKE_DISCORD_BOT_TOKEN,
        id="three_segment_value_without_discord_shape_or_context_is_not_redacted-bare",
    ),
    pytest.param(
        f"provider diagnostic value={FAKE_DISCORD_BOT_TOKEN}",
        id="three_segment_value_without_discord_shape_or_context_is_not_redacted-value",
    ),
    *(
        pytest.param(
            _bot_token(first_len=n),
            id=f"discord_bot_token_first_segment_out_of_range_is_not_redacted-{n}",
        )
        for n in (23, 29)
    ),
    *(
        pytest.param(
            _bot_token(middle_len=n),
            id=f"discord_bot_token_middle_segment_wrong_length_is_not_redacted-{n}",
        )
        for n in (5, 7)
    ),
    pytest.param(
        _bot_token(last_len=26),
        id="discord_bot_token_last_segment_too_short_is_not_redacted",
    ),
    # The greedy quantifier takes at most 38 chars, then the trailing negative
    # lookahead ``(?![A-Za-z0-9_-]|\.[A-Za-z0-9_-])`` fails at every backtrack
    # position of a 39-char uniform-charset last segment.
    pytest.param(
        _bot_token(last_len=39),
        id="discord_bot_token_last_segment_too_long_fails_trailing_lookahead",
    ),
    *(
        pytest.param(line, id=f"discord_bot_token_shape_near_matches_are_not_redacted-{variant}")
        for variant, line in (
            ("first_not_mno", "P" + _FIRST[1:] + "." + _MIDDLE + "." + _LAST),
            ("middle_long", _FIRST + "." + _MIDDLE + "0" + "." + _LAST),
            ("middle_short", _FIRST + "." + _MIDDLE[:-1] + "." + _LAST),
            ("first_short", _FIRST[:-1] + "." + _MIDDLE + "." + _LAST),
            ("last_short", _FIRST + "." + _MIDDLE + "." + _LAST[:-1]),
            ("last_long", _FIRST + "." + _MIDDLE + "." + _LAST + "FAKEFAKEFAKE"),
            ("embedded", "FAKE" + FAKE_SHAPED_DISCORD_BOT_TOKEN),
            ("embedded_dash", "FAKE-" + FAKE_SHAPED_DISCORD_BOT_TOKEN),
            ("four_segments_after", FAKE_SHAPED_DISCORD_BOT_TOKEN + "." + "FAKE00"),
            ("four_segments_before", "FAKE00" + "." + FAKE_SHAPED_DISCORD_BOT_TOKEN),
        )
    ),
    pytest.param(
        "https://example.com/api/webhooks/" + FAKE_DISCORD_WEBHOOK_ID + "/FAKE00",
        id="non_discord_webhook_url_is_not_redacted-other_host",
    ),
    pytest.param(
        "https://discord.com/api/webhooks/FAKE00/FAKE00",
        id="non_discord_webhook_url_is_not_redacted-short_id",
    ),
    *(
        pytest.param(line, id=f"ordinary_three_segment_diagnostic_is_not_redacted-{variant}")
        for variant, line in (
            ("route", "retry route=worker.us-east-1.stable after backoff"),
            ("version", "bot 1.0.0 started"),
            ("bot_version", "the Bot v1.2.3 worker is healthy"),
        )
    ),
    *(
        pytest.param(line, id=f"discord_bot_token_near_matches_are_not_redacted-{variant}")
        for variant, line in (
            ("two_segments", TWO_SEGMENTS),
            ("four_segments", FOUR_SEGMENTS),
            ("header_two_segments", "Authorization: Bot " + TWO_SEGMENTS),
            ("header_four_segments", "Authorization: Bot " + FOUR_SEGMENTS),
        )
    ),
    pytest.param("label am_short stays visible", id="short_am_prefix_is_not_treated_as_an_api_key"),
    pytest.param(
        "mytoken=" + FAKE_EGRESS_SECRET, id="alphanumeric_token_suffix_is_not_an_assignment"
    ),
    pytest.param(
        f"session token_count=12 channel=email event_id={BENIGN_EVENT_ID} X-Request-Id: abc",
        id="benign_near_matches_are_preserved",
    ),
    pytest.param(
        "token=[REDACTED:secret_assignment]",
        id="secret_assignment_placeholder_is_idempotent",
    ),
]


@pytest.mark.parametrize("line", PRESERVED_CASES)
def test_preserved(line: str) -> None:
    assert redact_text(line) == line


@pytest.mark.parametrize(
    "secret",
    [
        pytest.param(
            FAKE_SHAPED_DISCORD_BOT_TOKEN, id="discord_shape_redaction_is_idempotent-token"
        ),
        pytest.param(FAKE_DISCORD_WEBHOOK_URL, id="discord_shape_redaction_is_idempotent-webhook"),
        pytest.param(
            FAKE_DISCORD_BOT_AUTHORIZATION, id="discord_bot_redaction_is_idempotent-authorization"
        ),
        pytest.param(
            FAKE_DISCORD_BOT_TOKEN_ASSIGNMENT, id="discord_bot_redaction_is_idempotent-assignment"
        ),
    ],
)
def test_redaction_is_idempotent(secret: str) -> None:
    once = redact_text(secret)

    assert once != secret
    assert redact_text(once) == once


# Each case: a log format and argument, the exact line the installed
# RedactingLogFilter lets through. The argument is interpolated only at format
# time, so this proves the filter redacts formatted args, not just msg.
FILTER_CASES = [
    pytest.param(
        "discarding prepared attachment: %s",
        f"GET {FAKE_S3_PRESIGNED_URL} returned 403",
        f"discarding prepared attachment: GET {REDACTED_S3_PRESIGNED_URL} returned 403",
        id="s3_presigned_url_in_exception_text_is_redacted_through_filter",
    ),
    pytest.param(
        "discarding prepared attachment: %s",
        f"GET {FAKE_GCS_PRESIGNED_URL} returned 403",
        f"discarding prepared attachment: GET {REDACTED_GCS_PRESIGNED_URL} returned 403",
        id="gcs_presigned_url_in_exception_text_is_redacted_through_filter",
    ),
    pytest.param(
        "ingress X-API-Key: %s",
        FAKE_CHANNEL_TOKEN,
        "ingress X-API-Key: [REDACTED:x_api_key]",
        id="installed_filter_redacts_formatted_args",
    ),
    pytest.param(
        "upstream Bearer %s rejected",
        FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX,
        "upstream [REDACTED:bearer_token] rejected",
        id="bearer_header_with_discord_shaped_token_and_suffix_is_redacted_through_filter",
    ),
    pytest.param(
        "upstream X-API-Key: %s rejected",
        FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX,
        "upstream X-API-Key: [REDACTED:x_api_key] rejected",
        id="x_api_key_header_with_discord_shaped_token_and_suffix_is_redacted_through_filter",
    ),
    pytest.param(
        "upstream token=%s rejected",
        FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX,
        "upstream token=[REDACTED:secret_assignment] rejected",
        id="token_assignment_with_discord_shaped_token_and_suffix_is_redacted_through_filter",
    ),
    pytest.param(
        "upstream password=%s rejected",
        FAKE_DISCORD_TOKEN_WITH_TRAILING_SUFFIX,
        "upstream password=[REDACTED:secret_assignment] rejected",
        id="password_assignment_with_discord_shaped_token_and_suffix_is_redacted_through_filter",
    ),
]


@pytest.mark.parametrize(("fmt", "arg", "expected"), FILTER_CASES)
def test_installed_filter_redacts(
    fmt: str, arg: str, expected: str, request: pytest.FixtureRequest
) -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingLogFilter())
    logger = logging.getLogger(f"curie.telemetry.redact.{request.node.callspec.id}")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        logger.info(fmt, arg)
    finally:
        logger.removeHandler(handler)

    assert stream.getvalue() == expected + "\n"
