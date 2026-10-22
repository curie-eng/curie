"""A commissioning tuple bound to the owned TLS Valkey, @spec PROTECTED-COMMISSIONING-2/4.

The owned disposable TLS Valkey of this package runs at the pinned image with
its default user disabled. A tuple is built in hash order (B, then G holding
``sha256(B)``, then the manifest M naming ``sha256(G)``); its permit is issued
as the commissioning issuer and its journal started as the commissioning
control. Every principal is installed from its product ACL recipe under the
username its credential reference names. The ingress and worker suites share
it. Fixture IDs and secrets are placeholders; no real deployment data.
"""

from __future__ import annotations

import copy
import hashlib
import json
import secrets
import uuid
from pathlib import Path
from typing import Any

from . import test_commissioning_bootstrap as _bootstrap_vectors
from . import test_commissioning_operator_guard as _guard_vectors
from . import test_commissioning_records as _permit_vectors

HOOK = "alerts"
ROLES = ("issuer", "control", "candidate_enqueue", "candidate_worker")
COUNTERS = ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
PRODUCTION_FILES = (
    "bootstrap.json",
    "enqueue.json",
    "source_writer.json",
    "worker.json",
    "execution_config.json",
)


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def broker_now_ms(broker: Any) -> int:
    seconds, micros = broker.command("TIME")
    return int(seconds) * 1000 + int(micros) // 1000


def snapshot(broker: Any) -> dict[str, Any]:
    """Every key with its type, value and TTL, so a refusal is proved to change nothing."""
    result: dict[str, Any] = {}
    for raw in broker.command("KEYS", "*"):
        key = raw.decode() if isinstance(raw, bytes) else raw
        kind = broker.command("TYPE", key)
        kind = kind.decode() if isinstance(kind, bytes) else kind
        if kind == "stream":
            value: Any = broker.command("XRANGE", key, "-", "+")
        else:
            value = broker.command("GET", key)
        result[key] = (kind, value, broker.command("PTTL", key))
    return result


def connections(broker: Any) -> int:
    return int(broker.command("INFO", "stats")["total_connections_received"])


class CommissioningTuple:
    """B, G, M and one permit bound to the owned broker, in the specification's hash order."""

    def __init__(
        self,
        broker: Any,
        *,
        bootstrap: dict[str, Any] | None = None,
        guard: dict[str, Any] | None = None,
        **permit_changes: Any,
    ) -> None:
        from curie_protected_hooks.authority_records import parse_manifest
        from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
        from curie_protected_hooks.commissioning_provisioner import (
            COMMISSIONING_ENTRY_POINT,
            GUARD_PRINCIPALS,
            PRODUCTION_ENTRY_POINT,
        )
        from curie_protected_hooks.commissioning_records import parse_commissioning_permit

        self.broker = broker
        m = broker.manifest().as_dict()
        b = _bootstrap_vectors.bootstrap_value()
        for member in ("runtime_id", "runtime_generation", "execution_config_digest"):
            b[member] = m[member]
        b["production_credentials"] = copy.deepcopy(m["credential_refs"])
        b["entry_points"] = {
            "production": PRODUCTION_ENTRY_POINT,
            "commissioning": COMMISSIONING_ENTRY_POINT,
        }
        b["max_permit_ms"] = "86400000"
        b.update(bootstrap or {})
        self.bootstrap = parse_commissioning_bootstrap(canonical(b))
        credentials = {
            **b["production_credentials"],
            "issuer": b["issuer_identity"],
            "control": b["control_identity"],
            "candidate_enqueue": b["candidate_credentials"]["enqueue"],
            "candidate_worker": b["candidate_credentials"]["worker"],
            "qualification_publisher": b["qualification_publisher_identity"],
            "control_reader": b["control_reader_identity"],
            "source_writer": b["source_writer_identity"],
            "observer": {"id": "commissioning/example-observer", "generation": "1"},
        }
        g = {
            "schema_version": 1,
            "kind": "commissioning_guard_config",
            "bootstrap_digest": self.bootstrap.digest,
            "admission_policy_sha256": "a" * 64,
            "rbac_sha256": "b" * 64,
            "network_policy_sha256": "c" * 64,
            "broker_acl": {
                name: {"credential": credentials[name], "rules_sha256": "d" * 64}
                for name in GUARD_PRINCIPALS
            },
        }
        g.update(guard or {})
        self.guard_config = canonical(g)
        m["guard_identity"]["config_sha256"] = hashlib.sha256(self.guard_config).hexdigest()
        self.manifest = parse_manifest(canonical(m))
        now = broker_now_ms(broker)
        p = _permit_vectors.permit_value()
        p.update(
            runtime_id=m["runtime_id"],
            runtime_generation=m["runtime_generation"],
            manifest_digest=self.manifest.digest,
            bootstrap_digest=self.bootstrap.digest,
            execution_config_digest=m["execution_config_digest"],
            broker_identity=copy.deepcopy(m["broker_identity"]),
            guard_identity=copy.deepcopy(m["guard_identity"]),
            issuer_identity=copy.deepcopy(b["issuer_identity"]),
            control_identity=copy.deepcopy(b["control_identity"]),
            candidate_credentials=copy.deepcopy(b["candidate_credentials"]),
            issued_at_ms=str(now - 1000),
            expires_at_ms=str(now + 600000),
        )
        p["limits"] = dict(p["limits"], **permit_changes.pop("limits", {}))
        p.update(permit_changes)
        self.value = p
        self.permit = parse_commissioning_permit(canonical(p))
        self.trusted = {
            "manifest": self.manifest,
            "bootstrap": self.bootstrap,
            "trusted_issuer": copy.deepcopy(b["issuer_identity"]),
            "trusted_control": copy.deepcopy(b["control_identity"]),
            "max_permit_ms": 86400000,
        }
        self.references = {role: credentials[role] for role in ROLES}
        # Every role's secret exists up front, so a tuple that is never installed
        # still renders its directory files.
        self.passwords = {role: secrets.token_hex(24) for role in ROLES}
        # Each scoped source signs with its own secret; the permit admits the first only.
        self.sources = [
            {
                "source": copy.deepcopy(source),
                "hook": HOOK,
                "secret": secrets.token_hex(24),
                "target_ref": b["target_scope"][index],
            }
            for index, source in enumerate(b["source_scope"])
        ]

    @property
    def suffix(self) -> str:
        return self.value["campaign_id"] + ":" + self.value["campaign_generation"]

    @property
    def journal_key(self) -> str:
        return "protected:commissioning:journal:" + self.suffix

    @property
    def stream(self) -> str:
        from curie_protected_hooks.commissioning_acl import derive_commissioning_lane

        return derive_commissioning_lane(self.permit).stream

    def install(self) -> None:
        from curie_protected_hooks.commissioning_acl import commissioning_acl_rules

        for role in ROLES:
            permit = self.permit if role.startswith("candidate") else None
            password = self.passwords[role]
            self.broker.command(
                "ACL",
                "SETUSER",
                self.references[role]["id"],
                "reset",
                "on",
                ">" + password,
                *commissioning_acl_rules(role, permit=permit),
            )

    def remove(self) -> None:
        users = {
            u.decode() if isinstance(u, bytes) else u for u in self.broker.command("ACL", "USERS")
        }
        for role in ROLES:
            username = self.references[role]["id"]
            if username in users:
                self.broker.command("CLIENT", "KILL", "USER", username, "SKIPME", "yes")
                self.broker.command("ACL", "DELUSER", username)

    async def session(self, role: str, *, terminal: bool = False) -> Any:
        from curie_protected_hooks.commissioning_journal import CommissioningSession
        from curie_protected_hooks.worker_transport import WorkerCredential

        return await CommissioningSession.connect(
            self.manifest,
            WorkerCredential(self.references[role]["id"], self.passwords[role]),
            self.broker.ca_pem,
            principal=copy.deepcopy(self.references[role]),
            terminal=terminal,
        )

    async def issue(self) -> None:
        from curie_protected_hooks.commissioning_journal import issue_permit

        issuer = await self.session("issuer")
        try:
            await issue_permit(issuer, self.permit, **self.trusted)
        finally:
            await issuer.close()

    async def journal(self, *, start: bool = True, terminal: bool = False) -> Any:
        """The control journal; prepared, and started unless ``start`` is false."""
        from curie_protected_hooks.commissioning_journal import CommissioningJournal

        session = await self.session("control", terminal=terminal)
        journal = await CommissioningJournal.open(session, self.permit, **self.trusted)
        if not terminal:
            state = await journal.prepare()
            if start:
                await journal.start(state.generation)
        return journal

    def ingress_file(self, **changes: Any) -> dict[str, Any]:
        value = {
            "schema_version": 1,
            "campaign": {
                "campaign_id": self.value["campaign_id"],
                "campaign_generation": self.value["campaign_generation"],
            },
            "candidate_enqueue": {
                "credential_ref": copy.deepcopy(self.references["candidate_enqueue"]),
                "username": self.references["candidate_enqueue"]["id"],
                "password": self.passwords["candidate_enqueue"],
            },
            "sources": copy.deepcopy(self.sources),
        }
        value.update(changes)
        return value

    def worker_file(self, role: str = "candidate_worker", **changes: Any) -> dict[str, Any]:
        value = {
            "schema_version": 1,
            "campaign": {
                "campaign_id": self.value["campaign_id"],
                "campaign_generation": self.value["campaign_generation"],
            },
            "candidate_worker": {
                "credential_ref": copy.deepcopy(self.references[role]),
                "username": self.references[role]["id"],
                "password": self.passwords[role],
            },
        }
        value.update(changes)
        return value

    def write_directory(self, directory: Path, **overrides: bytes) -> Path:
        """The provisioner-owned directory; production files exist but are unreadable."""
        directory.mkdir(mode=0o700)
        files = {
            "manifest.json": self.manifest.canonical_bytes,
            "ca.pem": str(self.broker.ca_pem).encode("ascii"),
            "commissioning_bootstrap.json": self.bootstrap.canonical_bytes,
            "guard_config.json": self.guard_config,
        }
        files.update(overrides)
        if "commissioning_ingress.json" not in files:
            files["commissioning_ingress.json"] = canonical(self.ingress_file())
        if "commissioning_worker.json" not in files:
            files["commissioning_worker.json"] = canonical(self.worker_file())
        for name, raw in files.items():
            path = directory / name
            path.write_bytes(raw)
            path.chmod(0o600)
        for name in PRODUCTION_FILES:
            path = directory / name
            path.write_bytes(b"{}")
            path.chmod(0o000)
        return directory

    def envelope(
        self,
        body: bytes,
        *,
        source: int = 0,
        target: str | None = None,
        operation: str | None = None,
        **campaign: Any,
    ) -> Any:
        """A campaign envelope for ``body`` bound to this permit."""
        from curie_protected_hooks.commissioning_admissions import campaign_binding
        from curie_protected_hooks.commissioning_campaign_records import parse_campaign_envelope

        operation = operation or str(uuid.uuid4())
        binding = campaign_binding(self.permit)
        binding.update(campaign)
        return parse_campaign_envelope(
            canonical(
                {
                    "schema_version": 1,
                    "record_type": "commissioning_envelope",
                    "campaign": binding,
                    "delivery": {
                        "delivery_id": str(uuid.uuid4()),
                        "attempt_id": operation,
                        "reservation_id": operation,
                        "request_digest": hashlib.sha256(b"request " + body).hexdigest(),
                        "payload_digest": hashlib.sha256(body).hexdigest(),
                        "payload_bytes": str(len(body)),
                        "source": copy.deepcopy(self.sources[source]["source"]),
                        "target_ref": target or self.sources[source]["target_ref"],
                    },
                    "tool_access": "read-only",
                }
            )
        )

    async def enqueue(
        self, control: Any, body: bytes = b'{"alert":"example"}', *, commit: bool = True
    ) -> tuple[str, Any, Any]:
        """One genuine delivery as the ingress makes it: charged, dispatched, added, committed.

        ``control`` is the started journal's ``CampaignReservations``.
        """
        from curie_protected_hooks.commissioning_admissions import CampaignAdmissions
        from curie_protected_hooks.commissioning_reservations import Charge

        record = self.envelope(body)
        delivery = record.delivery
        operation = delivery["reservation_id"]
        await control.reserve(
            operation, "delivery", delivery["request_digest"], Charge(deliveries=1)
        )
        assert (await control.dispatch(operation)).fresh
        enqueue = await self.session("candidate_enqueue")
        try:
            entry = await enqueue.execute(
                "XADD",
                self.stream,
                "*",
                "payload",
                body,
                "commissioning_envelope",
                record.canonical_bytes,
            )
        finally:
            await enqueue.close()
        admission = await CampaignAdmissions(control).commit(record, entry) if commit else None
        return entry, record, admission


class GuardProbe:
    """A real ``ChannelCampaignGuard`` over fake producers and a fake signed channel.

    The premeasured bootstrap, manifest and permit digests and the ACL baseline
    are this tuple's own; the other facts are fixed placeholders. Only fakes
    implement the ADR 0236 producers here, as in the guard's own tests; this
    proves the ingress binds and calls the guard, not that a campaign passed.
    """

    def __init__(self, campaign: CommissioningTuple, *, refuse: bool = False) -> None:
        from curie_protected_hooks.commissioning_operator_guard import ChannelCampaignGuard
        from curie_protected_hooks.commissioning_producers import (
            ACL_PRINCIPALS,
            OBSERVATION_FACTS,
            AclBaseline,
            ReobservationSet,
        )

        v = _guard_vectors
        premeasured = {
            name: hashlib.sha256(name.encode()).hexdigest() for name in OBSERVATION_FACTS
        }
        premeasured.update(
            bootstrap=campaign.bootstrap.digest,
            manifest=campaign.manifest.digest,
            permit=campaign.permit.digest,
        )
        self._producers = {name: v.Fact(name, premeasured[name]) for name in OBSERVATION_FACTS}
        baseline = AclBaseline(campaign.bootstrap, v.STATES)
        reader = v.Reader()
        reader.replies = {
            username: {
                "flags": ["on"],
                "passwords": sorted(v.hashes(name)),
                "commands": "-@all",
            }
            for name, (username, _) in baseline.principals.items()
            if name in ACL_PRINCIPALS
        }
        reader.replies[v.OBSERVER] = dict(v.OBSERVER_REPLY)
        self.transport = v.Transport(refuse=refuse)
        self.guard = ChannelCampaignGuard(
            premeasured=premeasured,
            control=ReobservationSet(tuple(self._producers.values())),
            acl=v.observer(reader, baseline=baseline),
            phase=lambda: v.CAMPAIGN,
            transport=self.transport,
        )

    @property
    def calls(self) -> int:
        """Calls that reached the guard's reobservation, refused or not."""
        return int(self._producers["bootstrap"].calls)

    def refuse(self) -> None:
        self.transport.refuse = True
