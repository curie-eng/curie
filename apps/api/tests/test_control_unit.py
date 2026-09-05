"""Unit tests for the kill-key format and budget validation (no I/O)."""

import uuid

import pytest
from curie_api.config import Settings
from curie_api.killswitch import KILL_CHANNEL, kill_key
from curie_api.schemas import BudgetConfig
from pydantic import ValidationError


def test_valkey_dsn_honors_the_password_override() -> None:
    # The compose VALKEY_PASSWORD knob must reach the DSN the API connects with.
    assert Settings(valkey_password="s3cret").valkey_dsn() == (
        "redis://:s3cret@localhost:26379/0"
    )
    # An explicit full URL overrides the parts.
    assert (
        Settings(valkey_url="redis://:x@other:1/2").valkey_dsn()
        == "redis://:x@other:1/2"
    )


def test_valkey_dsn_uses_rediss_scheme_when_tls_is_enabled() -> None:
    # #2315: a BYO TLS-only Valkey/Redis needs rediss://, otherwise redis-py
    # sends a cleartext connection to a store that never negotiates or
    # downgrades. Verified on redis-py 8.1.0: redis.from_url("rediss://...")
    # selects redis.connection.SSLConnection, redis://... selects Connection.
    assert Settings(valkey_tls=True).valkey_dsn() == (
        "rediss://:valkeypass@localhost:26379/0"
    )
    # Otherwise byte-identical to the plain scheme -- only the scheme changes.
    assert Settings().valkey_dsn() == "redis://:valkeypass@localhost:26379/0"


def test_valkey_url_still_wins_over_valkey_tls() -> None:
    # The escape hatch stays outright authoritative even when the new signal
    # is also set.
    assert (
        Settings(
            valkey_tls=True, valkey_url="redis://:x@other:1/2"
        ).valkey_dsn()
        == "redis://:x@other:1/2"
    )


# --- VALKEY_TLS env -> Settings.valkey_tls (the seam the chart actually
# drives; a field that only works via kwargs is not wired) ------------------


def test_valkey_tls_env_reaches_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALKEY_TLS", "true")
    assert Settings().valkey_tls is True


def test_valkey_tls_defaults_false_on_a_clean_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # VALKEY_TLS may be set in the ambient host/CI env; clear it explicitly so
    # this default-value assertion cannot be flipped by something outside the
    # test.
    monkeypatch.delenv("VALKEY_TLS", raising=False)
    assert Settings().valkey_tls is False


def test_kill_key_matches_the_seam_contract() -> None:
    agent_id = uuid.UUID("00000000-0000-0000-0000-000000000001")
    assert kill_key(agent_id) == "curie:kill:00000000-0000-0000-0000-000000000001"
    assert KILL_CHANNEL == "curie:kill-events"


def test_budget_allows_null_and_positive_values() -> None:
    assert BudgetConfig().max_usd_per_day is None
    ok = BudgetConfig(max_usd_per_day=5.0, max_output_tokens_per_run=1000)
    assert ok.max_usd_per_day == 5.0
    assert ok.max_output_tokens_per_run == 1000


def test_budget_rejects_non_positive_values() -> None:
    with pytest.raises(ValidationError):
        BudgetConfig(max_usd_per_day=0)
    with pytest.raises(ValidationError):
        BudgetConfig(max_output_tokens_per_run=-1)
