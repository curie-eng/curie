"""#3823: each claim boots with its own credential id.

``fresh_boot_credential`` re-signs the boot state tokens under a new ``cred``
and keeps every other claim, so a retry that claims again from the same env
never carries the credential its failed claim released.
"""

from __future__ import annotations

import base64
import json
import uuid
from base64 import urlsafe_b64decode
from typing import Any

import pytest
from curie_internal.sandbox_token import verify
from curie_worker import binding as binding_module
from curie_worker import caller_token
from curie_worker.binding import (
    HISTORY_TOKEN_ENV,
    MEMORY_TOKEN_ENV,
    BindingResolver,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig
from nacl.signing import SigningKey

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


# -- ADR 0270 decision 3 [D5]: the requester cct is re-minted at claim time ---------------

_CALLER_ENV = "CURIE_CONNECTOR_CALLER_TOKEN"
_ROUTES_ENV = "CURIE_CONNECTOR_ROUTE_TOKENS"
_STATE_ENVS = (HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV, "CURIE_STATE_TOKEN")
_SCOPED = {"connector_release": "curie", "connector_namespace": "curie"}
_TENANT = "33333333-3333-4333-8333-333333333333"
_ISSUER = "https://identity.example.test/directory"
_SUBJECT = "SyntheticRequesterSubject"
_RUN = "11111111-1111-4111-8111-111111111111"
_WORK_ITEM = "22222222-2222-4222-8222-222222222222"
_PRINCIPAL_FIELDS = {"issuer": _ISSUER, "subject": _SUBJECT, "tenant_id": _TENANT}
# A frozen wall clock, far from the real one so no assertion passes by accident.
T = 1_900_000_000
REQUESTER_TTL = 3600


def _seed() -> str:
    """A fresh worker Ed25519 seed. @spec CONNECTOR-REQUESTER-3."""
    return base64.b64encode(bytes(SigningKey.generate())).decode()


def _principal() -> caller_token.CallerPrincipal:
    """@spec CONNECTOR-REQUESTER-3."""
    return caller_token.CallerPrincipal(tenant_id=_TENANT, issuer=_ISSUER, subject=_SUBJECT)


def _signing_resolver(seed: str, api_key: str = _KEY) -> BindingResolver:
    """A resolver holding the caller signing key, scoped so it mints a cct.

    @spec CONNECTOR-REQUESTER-3.
    """
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig(  # type: ignore[attr-defined]
        api_key=api_key, connector_caller_signing_key=seed, **_SCOPED
    )
    return resolver


def _boot_at(
    monkeypatch: pytest.MonkeyPatch, now: float, resolver: BindingResolver, **kwargs: Any
) -> dict[str, str]:
    """``boot_env`` as the delivery ran it at ``now``. @spec CONNECTOR-REQUESTER-3."""
    monkeypatch.setattr(binding_module.time, "time", lambda: now)
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
        resolved, "thread-1", kind="slack", address="C0EXAMPLE1", token_ttl_s=120, **kwargs
    )


def _remint_at(
    monkeypatch: pytest.MonkeyPatch, now: float, resolver: BindingResolver, env: dict[str, str]
) -> dict[str, str]:
    """``fresh_boot_credential`` as the substrate calls it at claim time ``now``.

    @spec CONNECTOR-REQUESTER-3.
    """
    monkeypatch.setattr(binding_module.time, "time", lambda: now)
    return resolver.fresh_boot_credential(env)


def _b64url(raw: bytes) -> str:
    """@spec CONNECTOR-REQUESTER-3."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _signed_cct(seed: str, body: dict[str, Any]) -> str:
    """A cct over any payload, signed with ``seed``: shapes the minter refuses.

    @spec CONNECTOR-REQUESTER-3.
    """
    payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("ascii")
    head = f"cct.{_b64url(payload)}"
    signature = SigningKey(base64.b64decode(seed)).sign(head.encode("ascii")).signature
    return f"{head}.{_b64url(signature)}"


def _verified_cct(seed: str, token: str) -> dict[str, Any]:
    """Signature-checked claims of a cct. @spec CONNECTOR-REQUESTER-3."""
    head, payload, signature = token.split(".")
    assert head == "cct"
    SigningKey(base64.b64decode(seed)).verify_key.verify(
        f"{head}.{payload}".encode("ascii"),
        urlsafe_b64decode(signature + "=" * (-len(signature) % 4)),
    )
    return _claims(token)


# @spec CONNECTOR-REQUESTER-3
def test_a_requester_cct_is_reminted_at_claim_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every boot carries a requester cct minted when the claim is made.

    Same principal field for field and same agent, expiry one hour after the
    claim, signed by the worker key. Only the cct changes; the caller keeps
    the delivery's token, so a second claim re-mints afresh.
    """
    seed = _seed()
    resolver = _signing_resolver(seed)
    env = _boot_at(
        monkeypatch,
        float(T),
        resolver,
        caller_principal=_principal(),
        route_connectors=frozenset({"reviewer"}),
    )
    original = env[_CALLER_ENV]
    before = dict(env)

    fresh = _remint_at(monkeypatch, T + 500.5, resolver, env)
    token = fresh[_CALLER_ENV]
    assert token != original
    assert _verified_cct(seed, token) == {
        "agent": "test-agent",
        "exp": T + 500 + REQUESTER_TTL,
        "principal": _PRINCIPAL_FIELDS,
    }
    assert token == caller_token.mint(
        seed, agent="test-agent", exp=T + 500 + REQUESTER_TTL, principal=_principal()
    )
    # Nothing but the cct and the state tokens' credential id moves.
    assert set(fresh) == set(env)
    assert fresh[_ROUTES_ENV] == env[_ROUTES_ENV]
    for key in set(env) - {_CALLER_ENV, *_STATE_ENVS}:
        assert fresh[key] == env[key], key
    # The input is a copy the caller keeps.
    assert env == before

    later = _remint_at(monkeypatch, T + 2000.0, resolver, env)
    assert _verified_cct(seed, later[_CALLER_ENV])["exp"] == T + 2000 + REQUESTER_TTL
    assert env[_CALLER_ENV] == original

    # The agent is the token's own, not anything the resolver knows.
    other_agent = dict(env)
    other_agent[_CALLER_ENV] = caller_token.mint(
        seed, agent="second-agent", exp=T + 100, principal=_principal()
    )
    reminted = _remint_at(monkeypatch, T + 600.0, resolver, other_agent)
    assert _verified_cct(seed, reminted[_CALLER_ENV]) == {
        "agent": "second-agent",
        "exp": T + 600 + REQUESTER_TTL,
        "principal": _PRINCIPAL_FIELDS,
    }


_LEFT_ALONE = [
    "principal-less",
    "run-pair",
    "malformed-principal",
    "null-principal",
    "unreadable",
    "keyless",
]


# @spec CONNECTOR-REQUESTER-3
@pytest.mark.parametrize("case", _LEFT_ALONE)
def test_principal_less_run_pair_and_malformed_ccts_are_left_byte_identical(
    monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """Only a well-formed, principal-bearing, run-less cct is re-minted.

    A principal-less token keeps its 24 hour session bytes; a run pair's
    deadline cap belongs to the deadline re-sign and must never be undone; a
    malformed principal is refused by the proxy anyway; and without a caller
    signing key nothing can be minted. Each case carries a control: the same
    resolver re-mints a well-formed requester cct.
    """
    seed = _seed()
    resolver = _signing_resolver(seed)
    if case == "principal-less":
        env = _boot_at(monkeypatch, float(T), resolver)
    elif case == "run-pair":
        env = _boot_at(
            monkeypatch,
            float(T),
            resolver,
            caller_principal=_principal(),
            caller_run=_RUN,
            caller_work_item=_WORK_ITEM,
            caller_exp_ceiling=T + 900,
        )
    elif case in {"malformed-principal", "null-principal", "unreadable"}:
        env = _boot_at(monkeypatch, float(T), resolver)
        env[_CALLER_ENV] = (
            "cct.payload.signature"
            if case == "unreadable"
            else _signed_cct(
                seed,
                {
                    "agent": "test-agent",
                    "exp": T + REQUESTER_TTL,
                    "principal": (
                        None
                        if case == "null-principal"
                        else {"issuer": _ISSUER, "subject": _SUBJECT}
                    ),
                },
            )
        )
    else:
        env = _boot_at(monkeypatch, float(T), resolver, caller_principal=_principal())
        resolver = _signing_resolver("")
    assert _CALLER_ENV in env
    fresh = _remint_at(monkeypatch, T + 500.0, resolver, env)
    assert fresh[_CALLER_ENV] == env[_CALLER_ENV]

    control_resolver = _signing_resolver(seed)
    control = _boot_at(monkeypatch, float(T), control_resolver, caller_principal=_principal())
    reminted = _remint_at(monkeypatch, T + 500.0, control_resolver, control)
    assert reminted[_CALLER_ENV] != control[_CALLER_ENV]
    assert _verified_cct(seed, reminted[_CALLER_ENV])["exp"] == T + 500 + REQUESTER_TTL


# @spec CONNECTOR-REQUESTER-3
def test_a_requester_cct_signed_by_another_key_is_left_byte_identical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The re-mint never signs a principal it did not itself sign.

    As with the state tokens (a token this key did not sign is left as is),
    re-signing an unverified principal would turn the claim-time minter into
    a signing oracle for whatever reached the env. Control: the key that did
    sign it re-mints.
    """
    seed, other = _seed(), _seed()
    env = _boot_at(monkeypatch, float(T), _signing_resolver(other), caller_principal=_principal())
    fresh = _remint_at(monkeypatch, T + 500.0, _signing_resolver(seed), env)
    assert fresh[_CALLER_ENV] == env[_CALLER_ENV]
    owner = _remint_at(monkeypatch, T + 500.0, _signing_resolver(other), env)
    assert _verified_cct(other, owner[_CALLER_ENV])["exp"] == T + 500 + REQUESTER_TTL


# @spec CONNECTOR-REQUESTER-3
def test_the_requester_cct_is_reminted_without_an_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cct re-mint needs only the caller signing key; state tokens stay as they were."""
    seed = _seed()
    env = _boot_at(monkeypatch, float(T), _signing_resolver(seed), caller_principal=_principal())
    assert all(env.get(key) for key in _STATE_ENVS)
    fresh = _remint_at(monkeypatch, T + 500.0, _signing_resolver(seed, api_key=""), env)
    assert _verified_cct(seed, fresh[_CALLER_ENV]) == {
        "agent": "test-agent",
        "exp": T + 500 + REQUESTER_TTL,
        "principal": _PRINCIPAL_FIELDS,
    }
    for key in _STATE_ENVS:
        assert fresh[key] == env[key], key
