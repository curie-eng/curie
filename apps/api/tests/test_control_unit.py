"""Unit tests for the kill-key format and budget validation (no I/O)."""

import uuid

import pytest
from curie_api.config import Settings
from curie_api.killswitch import KILL_CHANNEL, kill_key
from curie_api.schemas import BudgetConfig
from pydantic import ValidationError


def test_valkey_dsn_honors_the_password_override() -> None:
    # The compose VALKEY_PASSWORD knob must reach the DSN the API connects with.
    assert Settings(valkey_password="s3cret").valkey_dsn() == ("redis://:s3cret@localhost:26379/0")
    # An explicit full URL overrides the parts.
    assert Settings(valkey_url="redis://:x@other:1/2").valkey_dsn() == "redis://:x@other:1/2"


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
