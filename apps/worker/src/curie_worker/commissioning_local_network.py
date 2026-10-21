"""The local Cilium network producer and its per-call network fact, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. The local Cilium producer measures preventive
network enforcement of a disposable local installation. It reuses the managed
CNI probe kit's behavioral cases, owned listeners, verdict rules and entry
encoding unchanged, and adds what is local to Cilium:
- the Cilium engine identity of ``commissioning_local_cilium`` brackets the
  kit's run, and so does the protected namespace's policy set; any change
  between the two reads refuses the measurement;
- its own required set. A local Docker-driver installation has no instance
  metadata service, so that sub-case does not apply. Off-cluster egress is
  measured against an owned fixture on the node network, supplied as the
  ``cloud_network`` destination.

ADR 0236 binds the network fact into every signed guard reply. Re-running the
behavioral cases takes minutes, so each fact call reobserves what binds the
measurement instead: the engine identity and the policy set. Any difference,
or a measurement that did not pass every local entry, refuses. This module
implements the commissioning ``FactProducer`` protocol structurally;
production composition never imports it.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from .commissioning_local_cilium import (
    CILIUM_AGENT_OBJECTS,
    CILIUM_POLICY_COLLECTIONS,
    CiliumIdentity,
    CiliumReader,
    observe_cilium,
)
from .managed_cni_cases import (
    EntryKey,
    KitResult,
    ProbeCluster,
    ProbeKit,
    ProbeLayout,
    _Run,
    policy_snapshot,
    required_entries,
)
from .managed_cni_probe_kit import (
    PRODUCER_CONSTANTS,
    SUB_CASES,
    Measurement,
    ObservationEntry,
    PolicySet,
    ProbeKitRefused,
    accepts,
    channel_digest,
    refuse,
    require,
)

LOCAL_PRODUCER_KIND = "local-cilium-v1"
# Sub-cases this producer does not measure even within its own cases: a local
# Docker-driver install has no instance metadata service.
NOT_APPLICABLE_SUB_CASES = frozenset({"instance_metadata"})
# The local Cilium network fact measures cases 1, 6 and 8 only. Case 2 belongs to
# the hostile fact and case 7 joins when local case 7 is wired; both arrive with
# the peer slice.
LOCAL_NETWORK_CASES = (1, 6, 8)
# Allowlist, not a denylist: exactly the sub-cases of those cases the fact
# measures today. Everything else the shared kit returns -- cases 2, 3, 4, 5, 7
# and any future kit case -- is dropped before the digest and before the verdict,
# so a later kit case cannot leak into the local fact or its refusal.
_LOCAL_SUB_CASES = frozenset(
    (case, sub_case)
    for case in LOCAL_NETWORK_CASES
    for sub_case in SUB_CASES[case]
    if sub_case not in NOT_APPLICABLE_SUB_CASES
)
# ADR 0236 fact name -> probe kit channel and the cases this producer measures
# for it. The hostile and retained-peer facts follow with the peer slice.
FACTS = {"network": ("network", (1, 6, 8))}


def _in_local_scope(case: int, sub_case: str) -> bool:
    return (case, sub_case) in _LOCAL_SUB_CASES


def _keep_entry(item: ObservationEntry) -> bool:
    """Keep only this producer's cases. Cases 2, 3, 4, 5, 7 and any future kit case
    are dropped unconditionally. Instance metadata is not measured on a local
    install, so an ``unavailable``/``destination_unknown`` entry is dropped, but a
    real observation is kept so a metadata leak still fails the measurement."""
    if _in_local_scope(item.case, item.sub_case):
        return True
    if item.case == 6 and item.sub_case == "instance_metadata":
        return (item.outcome, item.reason) != ("unavailable", "destination_unknown")
    return False
# Kit refusals that stop before the remaining cases; the local filter keeps them.
_EARLY = ("enforcement_absent", "non_vacuity_unavailable")


class LocalCluster(ProbeCluster, CiliumReader, Protocol):
    """The measuring identity's client with the fixed Cilium reads."""


class _Kit(Protocol):
    @property
    def last(self) -> KitResult | None: ...

    async def measure(self) -> Measurement: ...


def local_required_entries(layout: ProbeLayout) -> frozenset[EntryKey]:
    return frozenset(
        key for key in required_entries(layout) if _in_local_scope(key[0], key[1])
    )


@dataclass(frozen=True, slots=True)
class LocalResult:
    measurement: Measurement
    identity: CiliumIdentity | None
    policies: PolicySet | None
    kit: KitResult | None


class LocalCiliumNetworkProducer:
    """Implements ``BehavioralNetworkProducer`` for a local Cilium installation."""

    producer_kind = LOCAL_PRODUCER_KIND

    def __init__(
        self,
        cluster: LocalCluster,
        layout: ProbeLayout,
        *,
        profile: str,
        kit: _Kit | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        require(layout.provider_policy_collections == CILIUM_POLICY_COLLECTIONS)
        require(set(CILIUM_AGENT_OBJECTS) <= set(layout.policy_agent_objects))
        require(not set(layout.supplied_egress) & NOT_APPLICABLE_SUB_CASES)
        require(bool(layout.supplied_egress.get("cloud_network")))
        # Both placements need a second node; one node cannot qualify.
        require(len(layout.eligible_nodes) >= 2)
        self._cluster, self._layout, self._profile = cluster, layout, profile
        self._clock, self._sleep = clock, sleep
        self._kit: _Kit = (
            kit if kit is not None else ProbeKit(cluster, layout, clock=clock, sleep=sleep)
        )
        self.last: LocalResult | None = None

    async def _identity(self) -> CiliumIdentity:
        return await observe_cilium(
            self._cluster,
            profile=self._profile,
            eligible_nodes=self._layout.eligible_nodes,
        )

    async def _policies(self) -> PolicySet | None:
        # The kit's own policy-set read: protected NetworkPolicies and Cilium's collections.
        return await policy_snapshot(_Run(self._cluster, self._layout, self._clock, self._sleep))

    async def measure(self) -> Measurement:
        result = await self._measure()
        self.last = result
        return result.measurement

    async def _measure(self) -> LocalResult:
        try:
            before = await self._identity()
        except ProbeKitRefused:
            return LocalResult(Measurement((), "cni_unavailable"), None, None, None)
        if set(before.families) != set(self._layout.families):
            return LocalResult(Measurement((), "families_unserved"), before, None, None)
        policies = await self._policies()
        if policies is None:
            return LocalResult(Measurement((), "policy_set_unavailable"), before, None, None)
        kit = await self._kit.measure()
        report = self._kit.last
        # Keep only this producer's own cases; drop every other kit case (including
        # future ones) before the digest and the verdict.
        entries = tuple(item for item in kit.entries if _keep_entry(item))
        try:
            after: CiliumIdentity | None = await self._identity()
        except ProbeKitRefused:
            after = None
        refusal = self._refusal(
            kit, report, entries, before, after, await self._policies() == policies
        )
        return LocalResult(Measurement(entries, refusal), before, policies, report)

    def _refusal(
        self,
        kit: Measurement,
        report: KitResult | None,
        entries: tuple[ObservationEntry, ...],
        before: CiliumIdentity,
        after: CiliumIdentity | None,
        policies_unchanged: bool,
    ) -> str | None:
        if report is None or report.leftovers:
            return "cleanup_unconfirmed"
        if after is None:
            return "cni_unavailable"
        if after.digest != before.digest:
            return "cni_changed"
        if not policies_unchanged:
            return "policy_set_changed"
        if kit.refusal in _EARLY:
            return kit.refusal
        failed = next((item for item in entries if item.outcome != "pass"), None)
        if failed is not None:
            return failed.reason
        required = local_required_entries(self._layout)
        # Exactly the local entries: none missing, none repeated, none extra.
        if not accepts(entries, required) or len(entries) != len(required):
            return "incomplete"
        return None

    def fact(self, name: str) -> LocalNetworkFact:
        require(name in FACTS)
        return LocalNetworkFact(self, name)

    async def _reobserve(self, name: str) -> str:
        channel, cases = FACTS[name]
        result = self.last
        require(result is not None)
        assert result is not None
        require(result.measurement.refusal is None and result.identity is not None)
        assert result.identity is not None
        entries = [item for item in result.measurement.entries if item.case in cases]
        required = {key for key in local_required_entries(self._layout) if key[0] in cases}
        require(accepts(entries, required))
        current = await self._identity()
        require(current.digest == result.identity.digest)
        require(await self._policies() == result.policies)
        return channel_digest(entries, channel)


class LocalNetworkFact:
    """One ADR 0236 fact, reobserved on every call; implements ``FactProducer``."""

    def __init__(self, producer: LocalCiliumNetworkProducer, name: str) -> None:
        self._producer, self._name = producer, name

    @property
    def fact(self) -> str:
        return self._name

    async def observe(self) -> str:
        try:
            async with asyncio.timeout(PRODUCER_CONSTANTS["per_call_deadline_ms"] / 1000):
                return await self._producer._reobserve(self._name)
        except ProbeKitRefused:
            raise
        except Exception:  # noqa: BLE001 -- an unavailable observation is a refusal.
            refuse()
