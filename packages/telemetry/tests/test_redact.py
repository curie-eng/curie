"""Redaction closes authentication formats before any telemetry export."""

from curie_telemetry.redact import redact_text


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
