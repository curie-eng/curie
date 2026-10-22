"""Commissioning worker entry, @spec PROTECTED-COMMISSIONING-2/4.

The provisioner-owned directory holds the shared ``manifest.json`` and
``ca.pem``, the commissioning bootstrap B (``commissioning_bootstrap.json``),
the guard configuration G (``guard_config.json``) and
``commissioning_worker.json``: exactly ``schema_version`` 1, the selected
``campaign`` (``campaign_id``, ``campaign_generation``) and the
``candidate_worker`` principal (``credential_ref``, ``username``,
``password``). The production ``worker.json`` and ``execution_config.json``
are never opened, and no environment variable, flag or label selects this
composition: the process either is it or is not.

Opening checks, in order, the first failure deciding with nothing after it
running: (1) this is the predeclared ``commissioning-worker-v1`` composition
and B's digest matches G and M; (2) the principal is B's candidate worker, its
session is pinned to M's server identity and ``run_id``, and ``ACL WHOAMI``
names it; (4) the issuer-written permit is this campaign's and its binding
holds at live broker time; (5) the campaign's ``ChannelCampaignGuard``,
premeasured for this B, M and permit, passes.
Step 3 has no worker-side capability (see ``commissioning_worker``). The
consumer then verifies every claim afresh and parks it. The production
``open_runtime`` is unchanged.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any, cast

from curie_protected_hooks.artifact_measurement import _open_root
from curie_protected_hooks.authority_records import (
    _CREDENTIAL,
    Manifest,
    _object,
    _pairs,
    _reject_number,
    parse_manifest,
)
from curie_protected_hooks.broker_transport import trusted_ca_pem
from curie_protected_hooks.commissioning_acl import PERMIT, derive_commissioning_lane
from curie_protected_hooks.commissioning_bootstrap import (
    CommissioningBootstrap,
    parse_commissioning_bootstrap,
)
from curie_protected_hooks.commissioning_operator_guard import ChannelCampaignGuard
from curie_protected_hooks.commissioning_provisioner import (
    COMMISSIONING_ENTRY_POINT,
    ENTRY_POINT_COMPOSITIONS,
    verify_tuple,
)
from curie_protected_hooks.commissioning_records import (
    BrokerObservation,
    CommissioningPermit,
    parse_commissioning_permit,
    validate_commissioning_binding,
)
from curie_protected_hooks.commissioning_worker import CampaignEntryGate, guard_measured_for
from curie_protected_hooks.runtime_directory import CA_FILE, MANIFEST_FILE
from curie_protected_hooks.worker_transport import (
    PROTECTED_WORKER_MAX_READ_BLOCK_MS,
    AuthenticatedWorkerClient,
    WorkerCredential,
)
from curie_worker.config import WorkerConfig
from curie_worker.protected_worker_bootstrap import _read
from pydantic import AliasChoices

from .consumer import RECHECK_INTERVAL_S, CommissioningConsumer, Verdict

__all__ = [
    "COMPOSITION",
    "CommissioningWorker",
    "CommissioningWorkerRefused",
    "open_commissioning_worker",
]

# @spec PROTECTED-COMMISSIONING-2: this composition's name among B's predeclared entry points.
COMPOSITION = "commissioning-worker-v1"
BOOTSTRAP_FILE = "commissioning_bootstrap.json"
GUARD_FILE = "guard_config.json"
WORKER_FILE = "commissioning_worker.json"
_CAMPAIGN: dict[str, Any] = {"campaign_id": "uuid", "campaign_generation": "generation"}


class CommissioningWorkerRefused(Exception):
    """One fixed refusal naming only the deciding step, @spec PROTECTED-COMMISSIONING-4."""

    def __init__(self, step: int) -> None:
        """@spec PROTECTED-COMMISSIONING-4."""
        super().__init__("Commissioning worker refused")
        self.step = step


@dataclass(frozen=True, slots=True, repr=False)
class _Directory:
    manifest: Manifest
    ca_pem: str = field(repr=False)
    bootstrap: CommissioningBootstrap
    guard_config: bytes
    campaign: dict[str, str]
    credential_ref: dict[str, str]
    credential: WorkerCredential = field(repr=False)


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("refused")


def _load(directory: str) -> _Directory:
    """Every commissioning worker file under one non-symlink root, read once."""
    root = _open_root(directory)
    try:
        manifest = parse_manifest(_read(root, MANIFEST_FILE))
        ca_pem = trusted_ca_pem(_read(root, CA_FILE).decode("ascii"))
        bootstrap = parse_commissioning_bootstrap(_read(root, BOOTSTRAP_FILE))
        guard_config = _read(root, GUARD_FILE)
        value = json.loads(
            _read(root, WORKER_FILE).decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    finally:
        os.close(root)
    _require(
        type(value) is dict and value.keys() == {"schema_version", "campaign", "candidate_worker"}
    )
    _require(type(value["schema_version"]) is int and value["schema_version"] == 1)
    _object(value["campaign"], _CAMPAIGN)
    principal = value["candidate_worker"]
    _require(
        type(principal) is dict and principal.keys() == {"credential_ref", "username", "password"}
    )
    _object(principal["credential_ref"], _CREDENTIAL)
    _require(principal["username"] == principal["credential_ref"]["id"])
    _require(type(principal["password"]) is str)
    credential = WorkerCredential(principal["username"], principal["password"])
    return _Directory(
        manifest,
        ca_pem,
        bootstrap,
        guard_config,
        value["campaign"],
        principal["credential_ref"],
        credential,
    )


def _campaign_config(permit: CommissioningPermit, consumer_name: str) -> WorkerConfig:
    """Lane names from the permit and defaults only, never the ordinary environment."""
    lane = derive_commissioning_lane(permit)
    values = {
        name: member.get_default(call_default_factory=True)
        for name, member in WorkerConfig.model_fields.items()
    }
    values.update(
        key_prefix=lane.prefix,
        consumer_group=lane.group,
        consumer_name=consumer_name,
        stream=lane.stream,
        dead_letter_stream=lane.dead,
        max_attempts=1,
        read_block_ms=PROTECTED_WORKER_MAX_READ_BLOCK_MS,
    )
    # Populate every validation alias explicitly, as the production protected
    # composition does, so no defaulted field is filled from the process.
    selected: dict[str, Any] = {}
    for name, member in WorkerConfig.model_fields.items():
        alias = member.validation_alias
        if isinstance(alias, AliasChoices):
            alias = alias.choices[0]
        _require(alias is None or isinstance(alias, str))
        selected[alias if isinstance(alias, str) else name] = values[name]
    return WorkerConfig(**selected)


class CommissioningWorker:
    """An opened commissioning worker: its client and its campaign consumer."""

    def __init__(
        self,
        client: AuthenticatedWorkerClient,
        consumer: CommissioningConsumer,
        config: WorkerConfig,
    ) -> None:
        """Built only by ``open_commissioning_worker``."""
        self._client = client
        self._consumer = consumer
        self._config = config

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-4."""
        return "CommissioningWorker()"

    @property
    def consumer_config(self) -> WorkerConfig:
        """The lane configuration the consumer runs with."""
        return self._config

    @property
    def verdicts(self) -> dict[str, Verdict]:
        """The latest claim verdict per entry."""
        return self._consumer.verdicts

    async def run(self) -> None:
        """Consume the campaign lane until stopped."""
        await self._consumer.run()

    def request_stop(self) -> None:
        """Stop reading new entries; parked claims stay pending."""
        self._consumer.request_stop()

    async def close(self) -> None:
        """End the pinned session; nothing reconnects."""
        await self._client.close()


async def open_commissioning_worker(
    directory: str,
    *,
    guard: ChannelCampaignGuard | None = None,
    consumer_name: str = "commissioning-consumer-1",
    recheck_interval_s: float = RECHECK_INTERVAL_S,
) -> CommissioningWorker:
    """Open the worker over ``directory`` in refusal order, @spec PROTECTED-COMMISSIONING-4."""
    client: AuthenticatedWorkerClient | None = None
    step = 1
    try:
        d = await asyncio.to_thread(_load, directory)
        b = d.bootstrap.as_dict()
        _require(COMPOSITION in ENTRY_POINT_COMPOSITIONS[COMMISSIONING_ENTRY_POINT])
        _require(b["entry_points"]["commissioning"] == COMMISSIONING_ENTRY_POINT)
        verify_tuple(d.bootstrap, d.guard_config, d.manifest)
        step = 2
        _require(d.credential_ref == b["candidate_credentials"]["worker"])
        client = await AuthenticatedWorkerClient.connect(d.manifest, d.credential, d.ca_pem)
        whoami = await client.lane().runs.execute_command("ACL", "WHOAMI")  # type: ignore[no-untyped-call]
        _require(whoami == d.credential.username)
        observed = await client.observe()
        step = 4
        selected = d.campaign
        raw = await client.lane().runs.execute_command(  # type: ignore[no-untyped-call]
            "GET", PERMIT + selected["campaign_id"] + ":" + selected["campaign_generation"]
        )
        _require(type(raw) is str)
        permit = parse_commissioning_permit(raw.encode("ascii"))
        p = permit.as_dict()
        _require({k: p[k] for k in ("campaign_id", "campaign_generation")} == selected)
        validate_commissioning_binding(
            permit,
            manifest=d.manifest,
            bootstrap=d.bootstrap,
            broker=BrokerObservation(
                identity=d.manifest.as_dict()["broker_identity"], time_ms=observed.now_ms
            ),
            trusted_issuer=b["issuer_identity"],
            trusted_control=b["control_identity"],
            max_permit_ms=int(b["max_permit_ms"]),
        )
        _require(p["candidate_credentials"]["worker"] == d.credential_ref)
        gate = await CampaignEntryGate.open(
            client, permit=permit, manifest=d.manifest, bootstrap=d.bootstrap, guard=guard
        )
        step = 5
        _require(type(guard) is ChannelCampaignGuard)
        bound = cast(ChannelCampaignGuard, guard)
        _require(guard_measured_for(bound, d.bootstrap, d.manifest, permit))
        await bound.require_current()
        config = _campaign_config(permit, consumer_name)
        consumer = CommissioningConsumer(
            client=client, gate=gate, config=config, recheck_interval_s=recheck_interval_s
        )
        return CommissioningWorker(client, consumer, config)
    except Exception:  # noqa: BLE001  The refusal names only the deciding step.
        if client is not None:
            await client.close()
        raise CommissioningWorkerRefused(step) from None
