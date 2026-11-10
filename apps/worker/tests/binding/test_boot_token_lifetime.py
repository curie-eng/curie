"""#3823: the boot-env state token expires with the turn, plus a short grace.

Both boot tokens (the broad ``state`` token and the narrow ``state.app``
token) share one ``cred`` id and one expiry. The connector caller token does
not follow them.
"""

from __future__ import annotations

import json
import time
import uuid
from base64 import urlsafe_b64decode

from curie_worker.binding import (
    BOOT_TOKEN_GRACE_SECONDS,
    DEFAULT_EXECUTION_DEADLINE_SECONDS,
    SANDBOX_TOKEN_TTL_SECONDS,
    BindingResolver,
)
from curie_worker.config import WorkerConfig

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")


def _resolved():
    from curie_worker.binding import ResolvedDeployment

    return ResolvedDeployment(
        agent_id=_AGENT,
        agent_name="test-agent",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )


def _resolver() -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(api_key="test-api-key")  # type: ignore[attr-defined]
    return resolver


def _claims(token: str) -> dict[str, object]:
    segment = token.split(".")[1]
    payload = urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    claims: dict[str, object] = json.loads(payload)
    return claims


def test_boot_tokens_expire_at_the_turn_deadline_plus_grace() -> None:
    before = int(time.time())
    env = _resolver().boot_env(
        _resolved(), "thread-1", kind="slack", address="C0EXAMPLE1", token_ttl_s=120.2
    )
    after = int(time.time())
    history = _claims(env["CURIE_HISTORY_TOKEN"])
    state = _claims(env["CURIE_STATE_TOKEN"])
    memory = _claims(env["CURIE_MEMORY_TOKEN"])
    lifetime = 121 + BOOT_TOKEN_GRACE_SECONDS
    assert history["exp"] == state["exp"] == memory["exp"]
    assert before + lifetime <= history["exp"] <= after + lifetime
    assert history["exp"] < before + SANDBOX_TOKEN_TTL_SECONDS
    assert history["cred"] == state["cred"]
    assert isinstance(history["cred"], str)
    assert len(history["cred"]) == 32
    assert history["memory"] == "read"
    assert history["binding"] == "slack:C0EXAMPLE1"
    assert "cred" in state
    assert "memory" not in state


def test_omitted_budget_uses_the_default_deadline_not_a_day() -> None:
    before = int(time.time())
    env = _resolver().boot_env(_resolved(), "thread-1")
    after = int(time.time())
    exp = _claims(env["CURIE_HISTORY_TOKEN"])["exp"]
    lifetime = DEFAULT_EXECUTION_DEADLINE_SECONDS + BOOT_TOKEN_GRACE_SECONDS
    assert before + lifetime <= exp <= after + lifetime
    assert exp < before + SANDBOX_TOKEN_TTL_SECONDS


def test_a_longer_than_cap_budget_stays_inside_the_day() -> None:
    before = int(time.time())
    env = _resolver().boot_env(_resolved(), "thread-1", token_ttl_s=10 * SANDBOX_TOKEN_TTL_SECONDS)
    after = int(time.time())
    exp = _claims(env["CURIE_STATE_TOKEN"])["exp"]
    assert before + SANDBOX_TOKEN_TTL_SECONDS <= exp <= after + SANDBOX_TOKEN_TTL_SECONDS + 5
