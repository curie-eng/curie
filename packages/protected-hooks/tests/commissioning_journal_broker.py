"""Commissioning principals and records bound to the owned TLS Valkey,
@spec PROTECTED-COMMISSIONING-1/2/3.

Each principal is installed from the product ACL recipe under the username its
bootstrap credential reference names. Records are rebound to the fixture
manifest by digest. Fixture IDs are placeholders; no real deployment data.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import importlib.util
import json
import secrets
from typing import Any

import pytest

from .admission_broker import FixtureSecret
from .test_commissioning_bootstrap import bootstrap_value
from .test_commissioning_records import permit_value

ROLES = ("issuer", "control", "provisioner", "observer", "candidate_enqueue", "candidate_worker")
COUNTERS = ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
CAMPAIGN = "66666666-6666-4666-8666-666666666666"
OTHER_CAMPAIGN = "99999999-9999-4999-8999-999999999999"


def journal_module() -> Any:
    name = "curie_protected_hooks.commissioning_journal"
    assert importlib.util.find_spec(name) is not None, "commissioning journal not implemented"
    return importlib.import_module(name)


def encoded(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


def broker_now_ms(broker: Any) -> int:
    seconds, micros = broker.command("TIME")
    return int(seconds) * 1000 + int(micros) // 1000


class Campaign:
    """One campaign's records, bound to the fixture's current manifest."""

    def __init__(self, broker: Any, **changes: Any) -> None:
        from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
        from curie_protected_hooks.commissioning_records import parse_commissioning_permit

        self.broker = broker
        self.manifest = broker.manifest()
        m = self.manifest.as_dict()
        b = bootstrap_value()
        for member in ("runtime_id", "runtime_generation", "execution_config_digest"):
            b[member] = m[member]
        b["production_credentials"] = copy.deepcopy(m["credential_refs"])
        b["max_permit_ms"] = "86400000"
        self.bootstrap = parse_commissioning_bootstrap(encoded(b))
        now = broker_now_ms(broker)
        p = permit_value()
        p.update(
            runtime_id=m["runtime_id"],
            runtime_generation=m["runtime_generation"],
            manifest_digest=self.manifest.digest,
            bootstrap_digest=self.bootstrap.digest,
            execution_config_digest=m["execution_config_digest"],
            broker_identity=copy.deepcopy(m["broker_identity"]),
            guard_identity=copy.deepcopy(m["guard_identity"]),
            issued_at_ms=str(now - 1000),
            expires_at_ms=str(now + 600000),
        )
        limits = changes.pop("limits", None)
        if limits is not None:
            p["limits"] = dict(p["limits"], **limits)
        p.update(changes)
        self.value = p
        self.permit = parse_commissioning_permit(encoded(p))
        self.trusted = {
            "manifest": self.manifest,
            "bootstrap": self.bootstrap,
            "trusted_issuer": copy.deepcopy(b["issuer_identity"]),
            "trusted_control": copy.deepcopy(b["control_identity"]),
            "max_permit_ms": 86400000,
        }
        self.references = {
            "issuer": b["issuer_identity"],
            "control": b["control_identity"],
            "candidate_enqueue": b["candidate_credentials"]["enqueue"],
            "candidate_worker": b["candidate_credentials"]["worker"],
            "provisioner": {"id": "commissioning/example-provisioner", "generation": "1"},
            "observer": {"id": "commissioning/example-observer", "generation": "1"},
        }

    @property
    def suffix(self) -> str:
        return self.value["campaign_id"] + ":" + self.value["campaign_generation"]

    @property
    def permit_key(self) -> str:
        return "protected:commissioning:permit:" + self.suffix

    @property
    def journal_key(self) -> str:
        return "protected:commissioning:journal:" + self.suffix

    @property
    def floor_key(self) -> str:
        return "protected:commissioning:journal:floor:" + self.value["runtime_id"]


class Principals:
    """Installed commissioning principals named by their credential references."""

    def __init__(self, broker: Any) -> None:
        self.broker = broker
        self.passwords: dict[str, FixtureSecret] = {}
        self.campaign = Campaign(broker)

    def install(self) -> None:
        from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

        for role in ROLES:
            permit = self.campaign.permit if role.startswith("candidate") else None
            rules = commissioning_acl_rules(role, permit=permit)
            password = self.passwords.setdefault(role, FixtureSecret(secrets.token_hex(24)))
            self.broker.command(
                "ACL",
                "SETUSER",
                self.campaign.references[role]["id"],
                "reset",
                "on",
                ">" + password,
                *rules,
            )

    def remove(self) -> None:
        users = {
            u.decode() if isinstance(u, bytes) else u for u in self.broker.command("ACL", "USERS")
        }
        for role in ROLES:
            username = self.campaign.references[role]["id"]
            if username in users:
                self.broker.command("CLIENT", "KILL", "USER", username, "SKIPME", "yes")
                self.broker.command("ACL", "DELUSER", username)

    def credential(self, role: str) -> Any:
        from curie_protected_hooks.worker_transport import WorkerCredential

        return WorkerCredential(self.campaign.references[role]["id"], self.passwords[role])

    def reference(self, role: str) -> dict[str, Any]:
        return copy.deepcopy(self.campaign.references[role])

    def raw(self, role: str) -> Any:
        from redis import Redis
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        return Redis(
            host="127.0.0.1",
            port=self.broker.port,
            username=self.campaign.references[role]["id"],
            password=self.passwords[role],
            ssl=True,
            ssl_ca_certs=str(self.broker.private / "ca.crt"),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            socket_timeout=3,
            socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )

    async def session(self, role: str, *, terminal: bool = False, manifest: Any = None) -> Any:
        api = journal_module()
        return await api.CommissioningSession.connect(
            manifest if manifest is not None else self.broker.manifest(),
            self.credential(role),
            self.broker.ca_pem,
            principal=self.reference(role),
            terminal=terminal,
        )


@pytest.fixture
def principals(pworker_service):
    pworker_service.command("FLUSHDB")
    installed = Principals(pworker_service)
    try:
        installed.install()
        yield installed
    finally:
        installed.remove()
        pworker_service.command("FLUSHDB")


def snapshot(broker: Any) -> dict[str, Any]:
    """Every key with its value and TTL, so a refusal is proved to change nothing."""
    keys = sorted(k.decode() if isinstance(k, bytes) else k for k in broker.command("KEYS", "*"))
    return {key: (broker.command("GET", key), broker.command("PTTL", key)) for key in keys}


def stored_journal(broker: Any, campaign: Campaign) -> dict[str, Any]:
    raw = broker.command("GET", campaign.journal_key)
    assert raw is not None
    value: dict[str, Any] = json.loads(raw)
    return value


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()
