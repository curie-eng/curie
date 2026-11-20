"""Both sandbox adapters record the requester the submitted caller token names.

ADR 0270 decision 3: every boot records, on the handle and on the durable
route record, the requester its connector caller token (``cct``) names and
that token's expiry. A principal-bearing cct records ``sha256:<digest>`` over
the principal, a principal-less one records ``"none"``, and an env with no cct
at all records ``"absent"`` with no expiry. The facts come from the env the
substrate actually submitted to the adapter, so a claim through the Docker
client and a claim through the Kubernetes client record the same thing. The
route record carries both fields through its JSON round trip, and a record
written before they existed reads back None for both, which cannot prove a
requester match.

Real Valkey backs the affinity store. The Docker CLI and the Kubernetes control
plane are the faked external boundaries (``_RecordingDocker`` and
``FakeSandboxClient`` from the sandbox conftest). Signing keys and tokens are
minted at runtime.

@spec CONNECTOR-REQUESTER-3.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from aci_protocol import BootEnv
from curie_worker import caller_token
from curie_worker.binding import CONNECTOR_CALLER_TOKEN_ENV
from curie_worker.sandbox import (
    AffinityStore,
    ClaimView,
    RouteRecord,
    SandboxSubstrate,
    SandboxView,
    SubstrateConfig,
)
from nacl.signing import SigningKey

from .conftest import FakeSandboxClient, _FakeBundleStore, _RecordingDocker

_SESSION_ENV = BootEnv.env_key("session_id")
_DIGEST_DOMAIN = b"curie-cct-principal-v1\n"
_TENANT = "44444444-4444-4444-8444-444444444444"
_ISSUER = "https://identity.example.test/directory"
_ALICE = {"tenant_id": _TENANT, "issuer": _ISSUER, "subject": "SyntheticAliceSubject"}
# Fixed expiries: the substrate reads them from the token, never from a clock.
_REQUESTER_EXP = 1_900_003_600
_PLAIN_EXP = 1_900_086_400
_ADAPTERS = ["docker", "kubernetes"]
_CASES = ["principal", "principal-less", "no-cct"]


def _digest(principal: dict[str, str]) -> str:
    """The requester fact, computed independently of the worker. @spec CONNECTOR-REQUESTER-3."""
    body = json.dumps(principal, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
    return "sha256:" + hashlib.sha256(_DIGEST_DOMAIN + body.encode("ascii")).hexdigest()


def _seed() -> str:
    """A fresh worker Ed25519 seed, never committed. @spec CONNECTOR-REQUESTER-3."""
    return base64.b64encode(bytes(SigningKey.generate())).decode("ascii")


def _env(case: str) -> dict[str, str]:
    """The boot env one case submits. @spec CONNECTOR-REQUESTER-3."""
    seed = _seed()
    if case == "principal":
        token = caller_token.mint(
            seed,
            agent="synthetic-agent",
            exp=_REQUESTER_EXP,
            principal=caller_token.CallerPrincipal(**_ALICE),
        )
        return {CONNECTOR_CALLER_TOKEN_ENV: token}
    if case == "principal-less":
        token = caller_token.mint(seed, agent="synthetic-agent", exp=_PLAIN_EXP)
        return {CONNECTOR_CALLER_TOKEN_ENV: token}
    return {_SESSION_ENV: "synthetic-session"}


def _expected(case: str) -> tuple[str, int | None]:
    """@spec CONNECTOR-REQUESTER-3."""
    return {
        "principal": (_digest(_ALICE), _REQUESTER_EXP),
        "principal-less": ("none", _PLAIN_EXP),
        "no-cct": ("absent", None),
    }[case]


def _kubernetes(_monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Callable[[str], dict[str, str]]]:
    """The Kubernetes client fake and what each claim was submitted with.

    @spec CONNECTOR-REQUESTER-3.
    """
    client = FakeSandboxClient()
    return client, lambda claim: client.claims[claim].env


def _docker(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Callable[[str], dict[str, str]]]:
    """The Docker client over its recorded CLI, and what each claim was submitted with.

    The readiness reads answer as a bound, running container, as the
    existing local-substrate test does. @spec CONNECTOR-REQUESTER-3.
    """
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    submitted: dict[str, dict[str, str]] = {}
    create_claim = client.create_claim

    def recording_create_claim(
        name: str, *, env: dict[str, str] | None = None, **kwargs: Any
    ) -> None:
        submitted[name] = dict(env or {})
        create_claim(name, env=env, **kwargs)

    monkeypatch.setattr(client, "create_claim", recording_create_claim)
    monkeypatch.setattr(
        client,
        "get_claim",
        lambda name, *, request_timeout_seconds: ClaimView(
            name, True, name, datetime.now(UTC), None, None, None
        ),
    )
    monkeypatch.setattr(
        client,
        "get_sandbox",
        lambda name, *, request_timeout_seconds: SandboxView(
            name, True, "127.0.0.1", "Running", 8080
        ),
    )
    return client, submitted.__getitem__


_FACTORIES = {"docker": _docker, "kubernetes": _kubernetes}


# @spec CONNECTOR-REQUESTER-3
@pytest.mark.parametrize("case", _CASES)
@pytest.mark.parametrize("adapter", _ADAPTERS)
def test_a_claim_records_the_submitted_requester_on_either_adapter(
    affinity: AffinityStore,
    config: SubstrateConfig,
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
    case: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-3: the handle and the route record name the booted cct.

    A claim through ``SandboxSubstrate`` over each adapter records the
    requester fact and expiry of the env it submitted: a digest and the
    token's expiry, ``"none"`` and its expiry, or ``"absent"`` and None. The
    durable record read back from Valkey and the record's own JSON carry
    the same two fields.
    """
    client, submitted = _FACTORIES[adapter](monkeypatch)
    substrate = SandboxSubstrate(client, affinity, config)
    env = _env(case)
    thread = f"T-requester-{adapter}-{case}"
    expected = _expected(case)

    handle = substrate.claim(thread, env=env)

    assert submitted(handle.claim_name).get(CONNECTOR_CALLER_TOKEN_ENV) == env.get(
        CONNECTOR_CALLER_TOKEN_ENV
    )
    assert (handle.caller_requester, handle.caller_token_exp) == expected
    record = affinity.get(thread)
    assert record is not None and record.handle == handle
    assert (record.handle.caller_requester, record.handle.caller_token_exp) == expected

    raw = RouteRecord(handle=handle).to_json()
    stored = json.loads(raw)
    assert (stored["caller_requester"], stored["caller_token_exp"]) == expected
    restored = RouteRecord.from_json(raw).handle
    assert restored == handle
    assert (restored.caller_requester, restored.caller_token_exp) == expected


# @spec CONNECTOR-REQUESTER-3
@pytest.mark.parametrize("adapter", _ADAPTERS)
def test_a_legacy_route_record_reads_back_no_requester_facts(
    affinity: AffinityStore,
    config: SubstrateConfig,
    monkeypatch: pytest.MonkeyPatch,
    adapter: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-3: a record written before the fields existed proves nothing.

    The JSON of a requester-bearing route, with both fields removed as a
    worker before this change wrote it, reads back None for each, never
    ``"none"`` or ``"absent"``; every other field is unchanged.
    """
    client, _submitted = _FACTORIES[adapter](monkeypatch)
    substrate = SandboxSubstrate(client, affinity, config)
    thread = f"T-legacy-{adapter}"
    handle = substrate.claim(thread, env=_env("principal"))
    assert (handle.caller_requester, handle.caller_token_exp) == _expected("principal")

    payload = json.loads(RouteRecord(handle=handle).to_json())
    del payload["caller_requester"]
    del payload["caller_token_exp"]
    legacy = RouteRecord.from_json(json.dumps(payload)).handle

    assert legacy.caller_requester is None
    assert legacy.caller_token_exp is None
    assert (
        replace(
            legacy,
            caller_requester=handle.caller_requester,
            caller_token_exp=handle.caller_token_exp,
        )
        == handle
    )
