"""Managed CNI behavioral producer: entries, verdict rules and producer interface, ADR 0238.

@spec MANAGED-CNI-2 @spec MANAGED-CNI-3 @spec MANAGED-CNI-4. This module holds
the producer's observation entry, the clause 2 constants table, the verdict
rules every case shares, and a minimal producer-facing interface. The commissioning
producer protocols do not exist yet; they adopt ``BehavioralNetworkProducer``
and ``Measurement`` instead of redefining them.

Verdicts are fail closed. ``pass`` comes only from clean behavioral evidence.
A delivery is ``fail`` and anything the probe could not observe is
``unavailable``. The ``fail`` and ``unavailable`` literals exist so evidence
explains a refusal; neither ever makes one positive. A configuration read never
decides an outcome. The ``ManagedCniFact`` and its identity object (clause 1)
belong to a later slice and are not defined here.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Iterable, Sequence, Set
from dataclasses import dataclass
from typing import Any, Literal, NoReturn, Protocol

Outcome = Literal["pass", "fail", "unavailable"]
Verdict = tuple[Outcome, str]
EntryKey = tuple[int, str, str, str, str]

PRODUCER_KIND = "managed-behavioral-v1"

# Clause 2, "Producer constants". Changing one is a new producer kind.
PRODUCER_CONSTANTS: dict[str, int] = {
    "denial_tcp_attempts": 5,
    "denial_udp_attempts": 5,
    "attempt_interval_ms": 200,
    "connect_timeout_ms": 2000,
    "listener_window_ms": 5000,
    "pod_phase_timeout_ms": 120000,
    "first_packet_interval_ms": 50,
    "first_packet_duration_ms": 10000,
    "first_packet_repetitions": 10,
    "address_reuse_max_pods": 40,
    "address_reuse_max_ms": 900000,
    "retained_heartbeat_ms": 100,
    "retained_bound_ms": 10000,
    "per_call_deadline_ms": 4000,
}

FAMILIES = ("ipv4", "ipv6")
PLACEMENTS = ("same_node", "cross_node")
# Clause 4 literals for the cases this slice measures. Cases 3, 4, 5 and 7 add
# theirs with their slice; until then an entry for them is refused.
SUB_CASES: dict[int, frozenset[str]] = {
    1: frozenset({"non_vacuity"}),
    2: frozenset({"broker", "mediator", "guard_refusal", "rbac_refusal"}),
    3: frozenset({"spoof_live", "spoof_terminated"}),
    4: frozenset({"new_pod", "address_reuse"}),
    5: frozenset({"workload_replaced", "policy_changed"}),
    7: frozenset(
        {
            "node_origin",
            "rbac_node_capability",
            "admission_node_capability",
            "running_node_capability",
        }
    ),
    6: frozenset(
        {
            "internet",
            "instance_metadata",
            "kubernetes_api",
            "node_addresses",
            "cloud_network",
            "external_dns",
            "declared_peers",
        }
    ),
    8: frozenset({"rbac_policy", "all_policies_present"}),
}
SOURCE_KINDS: dict[int, frozenset[str]] = {
    1: frozenset({"foreign_namespace", "protected_unselected"}),
    2: frozenset({"foreign_namespace", "protected_unselected", "release_namespace", "none"}),
    3: frozenset({"raw_socket"}),
    4: frozenset({"foreign_namespace", "protected_unselected"}),
    5: frozenset({"retained_peer"}),
    7: frozenset({"node", "none"}),
    6: frozenset({"runner_policy"}),
    8: frozenset({"none"}),
}
# Entries that are not per-address carry family, placement and source kind ``none``.
NON_ADDRESS_SUB_CASES = frozenset(
    {
        "guard_refusal",
        "rbac_refusal",
        "rbac_policy",
        "all_policies_present",
        "rbac_node_capability",
        "admission_node_capability",
        "running_node_capability",
    }
)
# External egress destinations have no placement (clause 4, case 6).
UNPLACED_SUB_CASES = frozenset(
    {
        "internet",
        "instance_metadata",
        "kubernetes_api",
        "cloud_network",
        "external_dns",
        "declared_peers",
    }
)
REASONS = frozenset(
    {
        "denied",
        "delivered",
        "enforcement_absent",
        "non_vacuity_unavailable",
        "reply_received",
        "reachable",
        "unreachable_unattributed",
        "control_failed",
        "listener_overflow",
        "listener_incomplete",
        "phase_timeout",
        "no_family_address",
        "wrong_node",
        "single_eligible_node",
        "destination_unknown",
        "destination_unreached",
        "transport_unavailable",
        "guard_refused",
        "guard_admitted",
        "refused_outside_guard",
        "reviews_denied",
        "review_allowed",
        "review_unavailable",
        "policy_set_unchanged",
        "policy_set_changed",
        # Cases 3, 4, 5 and 7 (this slice).
        "admitted_before_propagation",
        "reuse_not_provoked",
        "address_reassigned",
        "retained_flow",
        "trigger_unconfirmed",
        "reaches",
        "blocked",
        "node_probe_unavailable",
        "node_capable_pod",
        "separate_authority_domain_required",
        # Case 5 workload_replaced: the original TCP sequence state cannot be
        # reproduced from user space, so a non-delivery cannot be claimed as a pass.
        "sequence_state_unavailable",
    }
)
# Clause 1, channel digests. The CNI digest is built from the fact (later slice).
CHANNEL_CASES = {"network": (1, 6, 7, 8), "hostile": (2, 3, 4), "retained_peer": (5,)}
_DIGEST = re.compile(r"[0-9a-f]{64}")
_GUARD_REFUSAL = re.compile(
    r'(?:[a-z.]+ "[a-z0-9.-]*" is forbidden: )?'
    r'(?:admission webhook "(?P<webhook>[^"]+)" denied the request'
    r"|ValidatingAdmissionPolicy '(?P<policy>[^']+)' with binding '[^']*' denied request)"
)


class ProbeKitRefused(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Managed CNI probe kit refused")


def refuse() -> NoReturn:
    raise ProbeKitRefused() from None


def require(condition: bool) -> None:
    if not condition:
        refuse()


def canonical_json(value: Any) -> bytes:
    """LANE-2 canonical ASCII JSON: sorted keys, no floats or non-finite numbers."""
    _no_floats(value)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


def _no_floats(value: Any) -> None:
    require(type(value) is not float)
    if isinstance(value, dict):
        for key, member in value.items():
            require(type(key) is str)
            _no_floats(member)
    elif isinstance(value, (list, tuple)):
        for member in value:
            _no_floats(member)


def sha256_canonical(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def producer_constants_digest() -> str:
    return sha256_canonical(PRODUCER_CONSTANTS)


@dataclass(frozen=True, slots=True)
class ObservationEntry:
    """One clause 1 observation entry: one case, family, placement and source kind."""

    case: int
    sub_case: str
    family: str
    placement: str
    source_kind: str
    outcome: Outcome
    reason: str
    evidence_digest: str

    def __post_init__(self) -> None:
        require(type(self.case) is int and self.case in SUB_CASES)
        require(self.sub_case in SUB_CASES[self.case])
        require(self.source_kind in SOURCE_KINDS[self.case])
        require(self.outcome in ("pass", "fail", "unavailable"))
        require(self.reason in REASONS)
        require(type(self.evidence_digest) is str and bool(_DIGEST.fullmatch(self.evidence_digest)))
        if self.sub_case in NON_ADDRESS_SUB_CASES:
            require((self.family, self.placement, self.source_kind) == ("none", "none", "none"))
            return
        require(self.family in FAMILIES and self.source_kind != "none")
        if self.sub_case in UNPLACED_SUB_CASES:
            require(self.placement == "none")
        else:
            require(self.placement in PLACEMENTS)

    @property
    def key(self) -> EntryKey:
        return (self.case, self.sub_case, self.family, self.placement, self.source_kind)

    def document(self) -> dict[str, Any]:
        return {
            "case": self.case,
            "sub_case": self.sub_case,
            "family": self.family,
            "placement": self.placement,
            "source_kind": self.source_kind,
            "outcome": self.outcome,
            "reason": self.reason,
            "evidence_digest": self.evidence_digest,
        }


def entries_document(entries: Iterable[ObservationEntry]) -> list[dict[str, Any]]:
    return sorted((item.document() for item in entries), key=lambda doc: canonical_json(doc))


def channel_digest(entries: Iterable[ObservationEntry], channel: str) -> str:
    """sha256 of the canonical sorted array of the entries one ADR 0236 digest covers."""
    require(channel in CHANNEL_CASES)
    cases = CHANNEL_CASES[channel]
    return sha256_canonical(entries_document(item for item in entries if item.case in cases))


def accepts(entries: Sequence[ObservationEntry], required: Set[EntryKey]) -> bool:
    """The verifier's rule: every required entry present once and ``pass``; nothing else not."""
    if not required:
        return False
    keys = [item.key for item in entries]
    if len(keys) != len(set(keys)):
        return False
    if any(item.outcome != "pass" for item in entries):
        return False
    return set(required) <= set(keys)


@dataclass(frozen=True, slots=True)
class Measurement:
    """What a producer returns: its entries and, when it stopped early, the refusal reason."""

    entries: tuple[ObservationEntry, ...]
    refusal: str | None


class BehavioralNetworkProducer(Protocol):
    """Minimal producer-facing interface; the commissioning producer protocols adopt it."""

    @property
    def producer_kind(self) -> str: ...

    async def measure(self) -> Measurement: ...


@dataclass(frozen=True, slots=True)
class DenialEvidence:
    """What one denial probe and its paired allowed control observed (clause 2)."""

    sender_ready: bool
    listener_ready: bool
    on_requested_node: bool
    family_address: bool
    listener_deliveries: int
    sockets_sampled: bool
    window_covered: bool
    overflow_moved: bool
    client_handshakes: int
    client_replies: int
    control_succeeded: bool


def judge_denial(evidence: DenialEvidence, *, delivery_reason: str = "delivered") -> Verdict:
    """Clause 2, "Verdict of one denial probe". Either side's delivery is a delivery."""
    if evidence.listener_deliveries or evidence.client_handshakes or evidence.client_replies:
        return ("fail", delivery_reason)
    if not evidence.family_address:
        return ("unavailable", "no_family_address")
    if not (evidence.sender_ready and evidence.listener_ready):
        return ("unavailable", "phase_timeout")
    if not evidence.on_requested_node:
        return ("unavailable", "wrong_node")
    if not (evidence.sockets_sampled and evidence.window_covered):
        return ("unavailable", "listener_incomplete")
    if evidence.overflow_moved:
        return ("unavailable", "listener_overflow")
    if not evidence.control_succeeded:
        return ("unavailable", "control_failed")
    return ("pass", "denied")


@dataclass(frozen=True, slots=True)
class EgressEvidence:
    """Case 6: a destination without an owned listener, judged at the client."""

    sender_ready: bool
    on_requested_node: bool
    family_address: bool
    handshakes: int
    replies: int
    # Errors that are neither a timeout nor a reply, such as a host-unreachable
    # report: the probe cannot attribute them to the policy engine.
    unattributed: int
    control_succeeded: bool
    # Every destination answered an unrestricted probe pod on the same node. A
    # destination that answers nobody would read as denied without any policy.
    destinations_reached: bool


def judge_egress(evidence: EgressEvidence) -> Verdict:
    if evidence.handshakes or evidence.replies:
        return ("fail", "reply_received")
    if not evidence.family_address:
        return ("unavailable", "no_family_address")
    if not evidence.sender_ready:
        return ("unavailable", "phase_timeout")
    if not evidence.on_requested_node:
        return ("unavailable", "wrong_node")
    if evidence.unattributed:
        return ("unavailable", "unreachable_unattributed")
    if not evidence.control_succeeded:
        return ("unavailable", "control_failed")
    if not evidence.destinations_reached:
        return ("unavailable", "destination_unreached")
    return ("pass", "denied")


def judge_reviews(results: Sequence[bool | None]) -> Verdict:
    """SubjectAccessReview set: ``pass`` only when every review answered and was denied."""
    if any(result is True for result in results):
        return ("fail", "review_allowed")
    if not results or any(result is None for result in results):
        return ("unavailable", "review_unavailable")
    return ("pass", "reviews_denied")


def judge_guard_dry_run(status: int | None, message: str, guard_names: Sequence[str]) -> Verdict:
    """Case 2 ``guard_refusal``. Only a refusal that names the guard shows its decision.

    Admission, or a refusal by RBAC or any other admission controller before the
    guard decides, is ``fail`` because the guard decision was not observed.
    """
    require(bool(guard_names) and all(type(name) is str and name for name in guard_names))
    if status is None or status >= 500:
        return ("unavailable", "review_unavailable")
    if 200 <= status < 300:
        return ("fail", "guard_admitted")
    # Only the API server's own prefix names the refusing controller; anything
    # after it is that controller's free text and may quote any other name.
    match = _GUARD_REFUSAL.match(message)
    if match is not None and (match["webhook"] or match["policy"]) in guard_names:
        return ("pass", "guard_refused")
    return ("fail", "refused_outside_guard")


PolicySet = frozenset[tuple[str, ...]]


def judge_policy_sets(snapshots: Sequence[PolicySet | None]) -> Verdict:
    """Case 8 ``all_policies_present``: the selecting policy set never changed."""
    if len(snapshots) < 2 or any(snapshot is None for snapshot in snapshots):
        return ("unavailable", "review_unavailable")
    if len(set(snapshots)) != 1:
        return ("fail", "policy_set_changed")
    return ("pass", "policy_set_unchanged")


@dataclass(frozen=True, slots=True)
class SpoofEvidence:
    """Case 3: a raw-socket sender forges a source address (clause 4)."""

    sender_ready: bool
    listener_ready: bool
    on_requested_node: bool
    forged_family_address: bool
    listener_deliveries: int
    sockets_sampled: bool
    window_covered: bool
    overflow_moved: bool
    control_succeeded: bool
    # ``spoof_terminated`` only: the forged address was handed to a pod before the probe.
    address_reassigned: bool = False


def judge_spoof(evidence: SpoofEvidence) -> Verdict:
    """Case 3. A spoofed sender never receives the reply, so only the twin decides.

    ``pass`` when the twin recorded no datagram and no socket entry for the forged
    address; a delivery is ``fail``. A ``spoof_terminated`` address that was reused
    before the probe no longer measures a recently terminated address, so it is
    ``unavailable``, never a pass.
    """
    if evidence.listener_deliveries:
        return ("fail", "delivered")
    if evidence.address_reassigned:
        return ("unavailable", "address_reassigned")
    if not evidence.forged_family_address:
        return ("unavailable", "no_family_address")
    if not (evidence.sender_ready and evidence.listener_ready):
        return ("unavailable", "phase_timeout")
    if not evidence.on_requested_node:
        return ("unavailable", "wrong_node")
    if not (evidence.sockets_sampled and evidence.window_covered):
        return ("unavailable", "listener_incomplete")
    if evidence.overflow_moved:
        return ("unavailable", "listener_overflow")
    if not evidence.control_succeeded:
        return ("unavailable", "control_failed")
    return ("pass", "denied")


@dataclass(frozen=True, slots=True)
class RetainedEvidence:
    """Case 5: a flow allowed when established must pass no data past the trigger."""

    established: bool
    listener_ready: bool
    on_requested_node: bool
    family_address: bool
    post_trigger_deliveries: int
    sockets_sampled: bool
    window_covered: bool
    overflow_moved: bool
    # The triggering event completed: the workload's absence was confirmed, or the
    # probe policy change was applied.
    trigger_confirmed: bool
    # The heartbeat was delivered while still allowed, so the flow truly worked.
    control_succeeded: bool
    # ``workload_replaced`` only: the terminated sender's address was reused.
    reuse_provoked: bool = True


def judge_retained(evidence: RetainedEvidence) -> Verdict:
    """Case 5. ``fail`` if any heartbeat or data arrives after the trigger plus the bound."""
    if evidence.post_trigger_deliveries:
        return ("fail", "retained_flow")
    if not evidence.reuse_provoked:
        return ("unavailable", "reuse_not_provoked")
    if not evidence.family_address:
        return ("unavailable", "no_family_address")
    if not (evidence.established and evidence.listener_ready):
        return ("unavailable", "phase_timeout")
    if not evidence.on_requested_node:
        return ("unavailable", "wrong_node")
    if not (evidence.sockets_sampled and evidence.window_covered):
        return ("unavailable", "listener_incomplete")
    if evidence.overflow_moved:
        return ("unavailable", "listener_overflow")
    if not evidence.trigger_confirmed:
        return ("unavailable", "trigger_unconfirmed")
    if not evidence.control_succeeded:
        return ("unavailable", "control_failed")
    return ("pass", "denied")


def judge_node_origin(*, observed: bool, reached: bool) -> Verdict:
    """Case 7 ``node_origin``: a recorded fact, not a denial.

    ``pass`` with ``reaches`` or ``blocked`` when the observation was made, and
    ``unavailable`` when it could not be. Most CNIs admit node-to-local-pod
    traffic for kubelet health checks, so ``reaches`` is the expected result.
    """
    if not observed:
        return ("unavailable", "node_probe_unavailable")
    return ("pass", "reaches" if reached else "blocked")


def judge_running_nodes(violations: int | None) -> Verdict:
    """Case 7 ``running_node_capability``: a non-system node-capable pod on an eligible node."""
    if violations is None:
        return ("unavailable", "review_unavailable")
    if violations:
        return ("fail", "node_capable_pod")
    return ("pass", "reviews_denied")


def address_family(address: str) -> str:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        refuse()
    return "ipv4" if parsed.version == 4 else "ipv6"


def served_families(
    protected_pod_ips: Iterable[str],
    protected_service_families: Iterable[str],
    eligible_node_internal_ips: Iterable[str],
) -> tuple[str, ...]:
    """Clause 3: the union of pod, Service and eligible node families."""
    served = {address_family(address) for address in protected_pod_ips}
    for family in protected_service_families:
        require(family in ("IPv4", "IPv6"))
        served.add(family.lower())
    served.update(address_family(address) for address in eligible_node_internal_ips)
    return tuple(sorted(served))
