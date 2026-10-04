"""ADR-0188 (#3623): the worker mints memory credentials scoped to the channel.

Two credentials, both ``scope="state"`` so an old API still accepts them:

* the long-lived boot-env token (``CURIE_MEMORY_TOKEN`` / ``CURIE_HISTORY_TOKEN``)
  carries ``{binding, memory: "read"}``;
* the per-turn token from ``BindingResolver.turn_memory_token`` carries
  ``{binding, memory: "write", sender, turn}`` and a short expiry. It is only
  minted when the agent has memory writes on, the turn names a binding, and the
  thread is not eval-isolated.

DB-free: both are exercised on a bare resolver with a real ``WorkerConfig``, like
``test_channel_memory_ref.py``. Claims are read back by an independent decoder
here (the signature checked by the module's ``verify``), so these tests pin the
wire claims, not the module's own ``decode``.
"""

from __future__ import annotations

import base64
import json
import math
import time
import uuid
from typing import Any

from curie_internal.sandbox_token import verify
from curie_worker.binding import (
    SANDBOX_TOKEN_TTL_SECONDS,
    BindingResolver,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_KEY = "curie-dev-key"
_THREAD = "thread-1"
# The most a turn credential may outlive the turn's own deadline: clock skew only.
_CLOCK_SKEW_S = 5


def _resolved(**overrides: object) -> ResolvedDeployment:
    fields: dict[str, object] = {
        "agent_name": "test-agent",
        "agent_id": _AGENT,
        "version_id": uuid.UUID("22222222-2222-4222-8222-222222222222"),
        "version_label": "v1",
        "bundle_ref": "bundles/x.zip",
        "max_usd_per_day": None,
        "max_output_tokens_per_run": None,
    }
    fields.update(overrides)
    return ResolvedDeployment(**fields)  # type: ignore[arg-type]


def _resolver(config: WorkerConfig | None = None) -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = config or WorkerConfig()  # type: ignore[attr-defined]
    return resolver


def _claims(token: str) -> dict[str, Any]:
    assert verify(token, _KEY, agent=str(_AGENT), scope="state") is True
    seg = token.split(".")[1]
    payload = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    assert isinstance(payload, dict)
    return payload


def _turn_token(
    resolved: ResolvedDeployment | None = None,
    *,
    config: WorkerConfig | None = None,
    kind: str | None = "slack",
    address: str | None = "C0123",
    thread_key: str = _THREAD,
    sender: str = "U0SENDER1",
    turn: str = "evt-0001",
    ttl_s: float = 300.0,
) -> str | None:
    return _resolver(config).turn_memory_token(
        resolved if resolved is not None else _resolved(memory_writes=True),
        kind=kind,
        address=address,
        thread_key=thread_key,
        sender=sender,
        turn=turn,
        ttl_s=ttl_s,
    )


def test_boot_env_state_token_carries_binding_and_read() -> None:
    env = _resolver().boot_env(
        _resolved(memory_writes=True), _THREAD, kind="slack", address="C0123"
    )
    memory = _claims(env["CURIE_MEMORY_TOKEN"])
    assert memory["binding"] == "slack:C0123"
    assert memory["memory"] == "read"
    # The long-lived token outlives the turn, so it never names a sender or turn.
    assert "sender" not in memory
    assert "turn" not in memory
    assert set(memory) == {"agent", "scope", "exp", "binding", "memory"}
    # History rides the same token (transcripts are #3767's change, not this one).
    assert env["CURIE_HISTORY_TOKEN"] == env["CURIE_MEMORY_TOKEN"]
    # The bundle-facing state.app token stays three-claim and unchanged.
    app = env["CURIE_STATE_TOKEN"]
    assert verify(app, _KEY, agent=str(_AGENT), scope="state.app") is True
    seg = app.split(".")[1]
    app_payload = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    assert set(app_payload) == {"agent", "scope", "exp"}


def test_boot_env_binding_claim_is_the_unquoted_binding_scope() -> None:
    # The claim is the same string as the API's ``_binding_scope`` and
    # ``workflow_state_entries.binding_scope``: raw kind and address, not the
    # URL-quoted path segments.
    env = _resolver().boot_env(_resolved(), _THREAD, kind="mail", address="ops/team@example.com")
    assert _claims(env["CURIE_MEMORY_TOKEN"])["binding"] == "mail:ops/team@example.com"


def test_unbound_boot_env_mints_null_binding() -> None:
    # A turn with no channel (a targetless cron) gets a credential that names
    # no binding: present as JSON null, so the API can tell it from a legacy
    # token, which has no ``memory`` claim at all.
    env = _resolver().boot_env(_resolved(memory_writes=True), _THREAD)
    claims = _claims(env["CURIE_HISTORY_TOKEN"])
    assert "binding" in claims
    assert claims["binding"] is None
    assert claims["memory"] == "read"
    # Kind without address (or the reverse) names no binding either.
    half = _resolver().boot_env(_resolved(), _THREAD, kind="slack")
    assert _claims(half["CURIE_HISTORY_TOKEN"])["binding"] is None


def test_turn_token_only_with_writes_and_binding() -> None:
    assert _turn_token() is not None
    # Writes off: no write credential at all.
    assert _turn_token(_resolved(memory_writes=False)) is None
    # No binding named: nothing to scope a write to.
    assert _turn_token(kind=None) is None
    assert _turn_token(address=None) is None
    assert _turn_token(kind=None, address=None) is None
    # No platform key configured (fake/local): nothing to sign with.
    assert _turn_token(config=WorkerConfig(api_key="")) is None


def test_turn_token_claims_and_bounded_expiry() -> None:
    before = int(time.time())
    token = _turn_token(sender="U0SENDER1", turn="evt-0001", ttl_s=300.4)
    after = int(time.time())
    assert token is not None
    claims = _claims(token)
    assert claims["agent"] == str(_AGENT)
    assert claims["scope"] == "state"
    assert claims["binding"] == "slack:C0123"
    assert claims["memory"] == "write"
    assert claims["sender"] == "U0SENDER1"
    assert claims["turn"] == "evt-0001"
    assert set(claims) == {"agent", "scope", "exp", "binding", "memory", "sender", "turn"}
    # Review M2: the credential ends with the turn. exp is at most now plus the
    # turn's remaining budget (rounded up to a whole second), with no grace
    # beyond the turn's own deadline; ``_CLOCK_SKEW_S`` allows only for skew.
    ttl = math.ceil(300.4)
    assert before + 300 <= claims["exp"] <= after + ttl + _CLOCK_SKEW_S

    # A turn limit longer than the long-lived token's lifetime is capped at it.
    before = int(time.time())
    long = _turn_token(ttl_s=10 * SANDBOX_TOKEN_TTL_SECONDS)
    after = int(time.time())
    assert long is not None
    exp = _claims(long)["exp"]
    assert before + SANDBOX_TOKEN_TTL_SECONDS <= exp <= after + SANDBOX_TOKEN_TTL_SECONDS + 5

    # And it is distinct from the long-lived boot-env credential.
    env = _resolver().boot_env(
        _resolved(memory_writes=True), _THREAD, kind="slack", address="C0123"
    )
    assert token != env["CURIE_MEMORY_TOKEN"]


def test_short_turn_token_expires_with_the_turn() -> None:
    # Review M2: a 20-second turn's credential is dead seconds after the turn,
    # not a minute (or a whole delivery budget) later.
    for ttl_s in (20.0, 1.0):
        before = int(time.time())
        token = _turn_token(ttl_s=ttl_s)
        after = int(time.time())
        assert token is not None
        exp = _claims(token)["exp"]
        assert before + int(ttl_s) <= exp <= after + int(ttl_s) + _CLOCK_SKEW_S, (
            ttl_s,
            exp - after,
        )


def test_blank_author_is_no_person() -> None:
    from curie_worker.binding import NO_PERSON

    assert NO_PERSON == "<no person>"
    for blank in ("", "   ", "\t\n"):
        token = _turn_token(sender=blank)
        assert token is not None
        assert _claims(token)["sender"] == NO_PERSON
    padded = _turn_token(sender="  U0SENDER1 ")
    assert padded is not None
    assert _claims(padded)["sender"] == "U0SENDER1"


def test_eval_isolated_turn_gets_no_turn_token() -> None:
    # #1909: an eval-isolated turn carries no memory at all, so no write
    # credential either, even with writes on and a binding named.
    assert _turn_token(thread_key="eval:case-1") is None
    # A thread that merely starts with "eval-" is a normal turn.
    assert _turn_token(thread_key="eval-case-1") is not None
