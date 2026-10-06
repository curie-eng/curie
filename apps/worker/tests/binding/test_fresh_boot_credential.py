"""#3823: each claim boots with its own credential id.

``fresh_boot_credential`` re-signs the boot state tokens under a new ``cred``
and keeps every other claim, so a retry that claims again from the same env
never carries the credential its failed claim released.
"""

from __future__ import annotations

import json
import uuid
from base64 import urlsafe_b64decode

from curie_worker.binding import (
    HISTORY_TOKEN_ENV,
    MEMORY_TOKEN_ENV,
    BindingResolver,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig
from curie_worker.sandbox_token import verify

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_KEY = "test-api-key"


def _resolver(api_key: str = _KEY) -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(api_key=api_key)  # type: ignore[attr-defined]
    return resolver


def _env(resolver: BindingResolver) -> dict[str, str]:
    resolved = ResolvedDeployment(
        agent_id=_AGENT,
        agent_name="test-agent",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )
    return resolver.boot_env(
        resolved, "thread-1", kind="slack", address="C0EXAMPLE1", token_ttl_s=120
    )


def _claims(token: str) -> dict[str, object]:
    segment = token.split(".")[1]
    claims: dict[str, object] = json.loads(urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
    return claims


def test_every_state_token_moves_to_one_new_credential() -> None:
    resolver = _resolver()
    env = _env(resolver)
    fresh = resolver.fresh_boot_credential(env)

    old = _claims(env[HISTORY_TOKEN_ENV])["cred"]
    new = _claims(fresh[HISTORY_TOKEN_ENV])["cred"]
    assert isinstance(new, str) and len(new) == 32
    assert new != old
    assert fresh[MEMORY_TOKEN_ENV] == fresh[HISTORY_TOKEN_ENV]
    assert _claims(fresh["CURIE_STATE_TOKEN"])["cred"] == new
    # Everything but the credential id is what the boot env minted.
    for key, scope in (
        (HISTORY_TOKEN_ENV, "state"),
        (MEMORY_TOKEN_ENV, "state"),
        ("CURIE_STATE_TOKEN", "state.app"),
    ):
        before, after = _claims(env[key]), _claims(fresh[key])
        assert {**before, "cred": new} == after
        assert verify(fresh[key], _KEY, agent=str(_AGENT), scope=scope) is True
    # The rest of the env is untouched, and the input is not modified.
    untouched = {k for k in env if k not in {HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV}}
    untouched.discard("CURIE_STATE_TOKEN")
    assert all(fresh[k] == env[k] for k in untouched)
    assert _claims(env[HISTORY_TOKEN_ENV])["cred"] == old


def test_two_calls_never_share_a_credential() -> None:
    resolver = _resolver()
    env = _env(resolver)
    first = resolver.fresh_boot_credential(env)
    second = resolver.fresh_boot_credential(first)
    assert _claims(first[HISTORY_TOKEN_ENV])["cred"] != _claims(second[HISTORY_TOKEN_ENV])["cred"]


def test_a_token_signed_by_another_key_is_left_alone() -> None:
    env = _env(_resolver("another-key"))
    assert _resolver().fresh_boot_credential(env) == env


def test_no_signing_key_changes_nothing() -> None:
    env = _env(_resolver())
    assert _resolver("").fresh_boot_credential(env) == env
