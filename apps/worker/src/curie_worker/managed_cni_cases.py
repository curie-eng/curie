"""Cases 1, 2, 6 and 8 of the managed CNI behavioral producer, ADR 0238.

@spec MANAGED-CNI-2 @spec MANAGED-CNI-3 @spec MANAGED-CNI-4. The cases run
through ``ProbeCluster``, which needs only the measuring identity's rights of
clause 5: probe pods and their logs in the probe, protected and release
namespaces, server-side dry runs, SubjectAccessReviews and cluster reads.

Case 1 runs first. If it does not pass in every served family and both
placements, no other case runs and the measurement refuses with
``enforcement_absent`` or ``non_vacuity_unavailable``. Cases 3, 4, 5 and 7, the
CNI identity fact and the probe registry with resumable cleanup belong to later
slices. This module removes the pods it created, by UID, before it returns.
"""

from __future__ import annotations

import asyncio
import ipaddress
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Protocol

from .managed_cni_probe_kit import (
    FAMILIES,
    PLACEMENTS,
    PRODUCER_CONSTANTS,
    PRODUCER_KIND,
    DenialEvidence,
    EgressEvidence,
    Measurement,
    ObservationEntry,
    PolicySet,
    ProbeKitRefused,
    Verdict,
    address_family,
    judge_denial,
    judge_egress,
    judge_guard_dry_run,
    judge_policy_sets,
    judge_reviews,
    refuse,
    require,
    sha256_canonical,
)
from .managed_cni_probe_programs import (
    TEMPLATE_LABEL,
    ListenerRecord,
    SenderRecord,
    parse_listener_log,
    parse_sender_log,
    probe_kit_digest,
    probe_pod,
)

# Case 6 fixed destinations, two per family (producer constants of this kind).
# Internet addresses must answer on both 443 and 80, or the reachability
# baseline would make the entry unavailable on every cluster.
INTERNET = {
    "ipv4": ("1.1.1.1", "1.0.0.1"),
    "ipv6": ("2606:4700:4700::1111", "2606:4700:4700::1001"),
}
RESOLVERS = {
    "ipv4": ("8.8.8.8", "9.9.9.9"),
    "ipv6": ("2001:4860:4860::8888", "2620:fe::fe"),
}
KUBELET_PORT = 10250
CLOSED_HIGH_PORT = 47913
# Sub-cases whose destinations come from the provider table or the bootstrap record.
SUPPLIED_EGRESS = ("instance_metadata", "kubernetes_api", "cloud_network")
_POLL_S = 1.0
NONE = ("none", "none", "none")
_CONCURRENCY = 4


@dataclass(frozen=True, slots=True)
class Subject:
    kind: str  # User, Group or ServiceAccount
    name: str
    namespace: str = ""

    def __post_init__(self) -> None:
        require(self.kind in ("User", "Group", "ServiceAccount") and bool(self.name))
        require(bool(self.namespace) == (self.kind == "ServiceAccount"))
        require("*" not in self.name)

    def review_identity(self) -> tuple[str, list[str]]:
        if self.kind == "User":
            return self.name, ["system:authenticated"]
        if self.kind == "Group":
            return "curie-probe:ordinary-group-member", [self.name]
        return (
            f"system:serviceaccount:{self.namespace}:{self.name}",
            [
                "system:serviceaccounts",
                f"system:serviceaccounts:{self.namespace}",
                "system:authenticated",
            ],
        )


@dataclass(frozen=True, slots=True)
class ResourceAttributes:
    verb: str
    group: str
    resource: str
    namespace: str = ""
    subresource: str = ""
    name: str = ""

    def document(self) -> dict[str, str]:
        optional = (
            ("namespace", self.namespace),
            ("subresource", self.subresource),
            ("name", self.name),
        )
        return {"verb": self.verb, "group": self.group, "resource": self.resource} | {
            key: value for key, value in optional if value
        }


BUILT_IN_GROUPS = (
    Subject("Group", "system:authenticated"),
    Subject("Group", "system:unauthenticated"),
    Subject("Group", "system:serviceaccounts"),
)


class ProbeCluster(Protocol):
    """What the cases need from the cluster, under the measuring identity only."""

    async def create_pod(self, namespace: str, document: dict[str, Any]) -> dict[str, Any]: ...

    async def read_pod(self, namespace: str, name: str) -> dict[str, Any] | None: ...

    async def pod_log(self, namespace: str, name: str) -> str: ...

    async def delete_pod(self, namespace: str, name: str, uid: str) -> None: ...

    async def dry_run_create_pod(
        self, namespace: str, document: dict[str, Any]
    ) -> tuple[int | None, str]: ...

    async def access_review(
        self, user: str, groups: Sequence[str], attributes: ResourceAttributes
    ) -> bool | None: ...

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None: ...


@dataclass(frozen=True, slots=True)
class Target:
    """A protected target and the policy facts its twin shares (clause 2, "Twins")."""

    name: str  # broker or mediator
    role_labels: Mapping[str, str]
    allowed_sender_labels: Mapping[str, str]
    port: int
    protocols: tuple[str, ...]
    closed_port: int  # a port no rule of the target's policy admits (case 1)
    real_addresses: tuple[str, ...]  # client-side addresses of the real target

    def __post_init__(self) -> None:
        require(self.name in ("broker", "mediator"))
        require(bool(self.role_labels) and bool(self.allowed_sender_labels))
        require(0 < self.port < 65536 and 0 < self.closed_port < 65536)
        require(self.port != self.closed_port)
        require(bool(self.protocols) and set(self.protocols) <= {"tcp", "udp"})
        for address in self.real_addresses:
            address_family(address)


@dataclass(frozen=True, slots=True)
class Destination:
    address: str
    port: int
    protocol: str  # tcp, udp, dns_tcp or dns_udp

    def __post_init__(self) -> None:
        address_family(self.address)
        require(0 < self.port < 65536 and self.protocol in ("tcp", "udp", "dns_tcp", "dns_udp"))


@dataclass(frozen=True, slots=True)
class ResourceRef:
    group: str
    resource: str
    namespace: str = ""
    name: str = ""


@dataclass(frozen=True, slots=True)
class ProbeLayout:
    """Everything the cases measure against; built by the caller from G and the bootstrap."""

    campaign: str
    protected_namespace: str
    probe_namespace: str
    foreign_namespace: str
    release_namespace: str
    eligible_nodes: tuple[str, ...]
    families: tuple[str, ...]
    targets: tuple[Target, ...]
    runner_labels: Mapping[str, str]
    declared_peers: tuple[Destination, ...]
    supplied_egress: Mapping[str, tuple[Destination, ...]]
    guard_names: tuple[str, ...]
    system_set: frozenset[Subject]
    excluded: frozenset[Subject]
    provider_policy_collections: tuple[str, ...]
    policy_agent_objects: tuple[ResourceRef, ...]

    def __post_init__(self) -> None:
        require(
            bool(self.eligible_nodes) and len(set(self.eligible_nodes)) == len(self.eligible_nodes)
        )
        require(bool(self.families) and set(self.families) <= set(FAMILIES))
        require(sorted(target.name for target in self.targets) == ["broker", "mediator"])
        require(bool(self.runner_labels) and bool(self.declared_peers) and bool(self.guard_names))
        require(set(self.supplied_egress) <= set(SUPPLIED_EGRESS))
        # Clause 6: an empty or wildcard system set refuses. ``Subject`` refuses a
        # literal wildcard; a group every request carries is a wildcard in effect.
        require(bool(self.system_set))
        require(not any(subject in self.system_set for subject in BUILT_IN_GROUPS))
        namespaces = (
            self.protected_namespace,
            self.probe_namespace,
            self.foreign_namespace,
            self.release_namespace,
        )
        require(len(set(namespaces)) == 4)

    def target(self, name: str) -> Target:
        return next(target for target in self.targets if target.name == name)


@dataclass(slots=True)
class _Pod:
    namespace: str
    name: str
    uid: str
    node: str
    addresses: tuple[str, ...]

    def address(self, family: str) -> str | None:
        return next((a for a in self.addresses if address_family(a) == family), None)


@dataclass(slots=True)
class _Run:
    cluster: ProbeCluster
    layout: ProbeLayout
    clock: Callable[[], float]
    sleep: Callable[[float], Awaitable[None]]
    created: list[tuple[str, str, str]] = field(default_factory=list)
    evidence: list[dict[str, Any]] = field(default_factory=list)
    counter: int = 0
    # Every nonce a denial probe or its control sent in this run.
    nonces: set[str] = field(default_factory=set)
    # One sender per node from creation until its listener window closes, so no
    # other probe pod of this run can take over its address inside the window.
    node_locks: dict[str, asyncio.Lock] = field(default_factory=dict)

    def node_lock(self, node: str) -> asyncio.Lock:
        return self.node_locks.setdefault(node, asyncio.Lock())


def _nonce() -> str:
    return secrets.token_hex(16)


EntryKey = tuple[int, str, str, str, str]


def _entry(
    run: _Run, key: EntryKey, *, verdict: Verdict, evidence: dict[str, Any]
) -> ObservationEntry:
    case, sub_case, family, placement, source_kind = key
    document = {
        "case": case,
        "sub_case": sub_case,
        "family": family,
        "placement": placement,
        "source_kind": source_kind,
        "outcome": verdict[0],
        "reason": verdict[1],
        "evidence": evidence,
    }
    run.evidence.append(document)
    return ObservationEntry(
        case=case,
        sub_case=sub_case,
        family=family,
        placement=placement,
        source_kind=source_kind,
        outcome=verdict[0],
        reason=verdict[1],
        evidence_digest=sha256_canonical(document),
    )


async def _start(
    run: _Run,
    *,
    template: str,
    namespace: str,
    node: str,
    labels: Mapping[str, str],
    argument: Mapping[str, Any],
    finished: bool,
) -> _Pod | None:
    """Create one probe pod and wait for Running (listener) or a terminal phase (sender)."""
    run.counter += 1
    name = f"cni-{run.layout.campaign}-{run.counter}-{secrets.token_hex(3)}"[:63].rstrip("-")
    document = probe_pod(
        template=template,
        name=name,
        namespace=namespace,
        node=node,
        campaign=run.layout.campaign,
        role_labels=labels,
        argument=argument,
    )
    created = await run.cluster.create_pod(namespace, document)
    uid = created["metadata"]["uid"]
    run.created.append((namespace, name, uid))
    deadline = run.clock() + PRODUCER_CONSTANTS["pod_phase_timeout_ms"] / 1000
    while run.clock() < deadline:
        pod = await run.cluster.read_pod(namespace, name)
        if pod is None or pod["metadata"]["uid"] != uid:
            refuse()
        status = pod.get("status", {})
        phase = status.get("phase")
        addresses = tuple(item["ip"] for item in status.get("podIPs", []) if item.get("ip"))
        observed = _Pod(namespace, name, uid, pod["spec"].get("nodeName", ""), addresses)
        if finished and phase in ("Succeeded", "Failed"):
            return observed
        if not finished and phase == "Running" and addresses:
            if parse_listener_log(await run.cluster.pod_log(namespace, name)).ready:
                return observed
        await run.sleep(_POLL_S)
    return None


def _attempts(
    destinations: Sequence[tuple[str, int, str]],
) -> tuple[list[dict[str, Any]], list[str]]:
    attempts, nonces = [], []
    for address, port, protocol in destinations:
        count = PRODUCER_CONSTANTS[
            "denial_udp_attempts" if protocol in ("udp", "dns_udp") else "denial_tcp_attempts"
        ]
        for _ in range(count):
            nonce = _nonce()
            nonces.append(nonce)
            attempts.append({"addr": address, "port": port, "proto": protocol, "nonce": nonce})
    return attempts, nonces


def _sender_argument(*phases: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "phases": list(phases),
        "interval_ms": PRODUCER_CONSTANTS["attempt_interval_ms"],
        "timeout_ms": PRODUCER_CONSTANTS["connect_timeout_ms"],
    }


async def _sender_record(run: _Run, pod: _Pod | None) -> SenderRecord | None:
    if pod is None:
        return None
    try:
        return parse_sender_log(await run.cluster.pod_log(pod.namespace, pod.name))
    except ProbeKitRefused:
        return None


def _pod_evidence(pod: _Pod | None) -> dict[str, Any] | None:
    if pod is None:
        return None
    return {"uid": pod.uid, "node": pod.node, "addresses": list(pod.addresses)}


def _sender_evidence(record: SenderRecord | None) -> list[list[Any]] | None:
    return None if record is None else [list(item) for item in record.attempts]


def _other_node(layout: ProbeLayout, node: str) -> str | None:
    return next((item for item in layout.eligible_nodes if item != node), None)


@dataclass(frozen=True, slots=True)
class _Source:
    kind: str
    template: str
    namespace: str


def _sources(layout: ProbeLayout, kinds: Sequence[str]) -> list[_Source]:
    table = {
        "foreign_namespace": _Source("foreign_namespace", "probe_sender", layout.foreign_namespace),
        "protected_unselected": _Source(
            "protected_unselected", "protected_probe", layout.protected_namespace
        ),
        "release_namespace": _Source(
            "release_namespace", "release_probe", layout.release_namespace
        ),
    }
    return [table[kind] for kind in kinds]


async def _listener_past(run: _Run, twin: _Pod, end_ms: int | None) -> ListenerRecord:
    """Read the listener until its own clock has ticked past ``end_ms``.

    Pod times come from the node clock, so the wait is judged by the listener's
    ticks rather than by this process's clock. A listener that never ticks past
    the window leaves it uncovered, which is ``unavailable``.
    """
    deadline = run.clock() + PRODUCER_CONSTANTS["listener_window_ms"] / 1000 + 30
    while True:
        record = parse_listener_log(await run.cluster.pod_log(twin.namespace, twin.name))
        if end_ms is None or any(tick[0] > end_ms for tick in record.ticks):
            return record
        if run.clock() > deadline:
            return record
        await run.sleep(_POLL_S)


async def _denial(
    run: _Run,
    *,
    case: int,
    sub_case: str,
    family: str,
    placement: str,
    source: _Source,
    twin: _Pod,
    target: Target,
    port: int,
    real: bool,
    delivery_reason: str,
) -> ObservationEntry:
    """One clause 2 denial probe at a twin (and the real target), then its allowed control."""
    layout = run.layout
    entry: EntryKey = (case, sub_case, family, placement, source.kind)
    node = twin.node if placement == "same_node" else _other_node(layout, twin.node)
    evidence: dict[str, Any] = {"listener": _pod_evidence(twin), "port": port}
    if node is None:
        return _entry(
            run, entry, verdict=("unavailable", "single_eligible_node"), evidence=evidence
        )
    twin_address = twin.address(family)
    if twin_address is None:
        return _entry(run, entry, verdict=("unavailable", "no_family_address"), evidence=evidence)
    destinations = [(twin_address, port, protocol) for protocol in target.protocols]
    if real:
        destinations += [
            (address, target.port, protocol)
            for address in target.real_addresses
            if address_family(address) == family
            for protocol in target.protocols
        ]
    attempts, nonces = _attempts(destinations)
    control_attempts, control_nonces = _attempts(
        [(twin_address, target.port, protocol) for protocol in target.protocols]
    )
    run.nonces.update(nonces, control_nonces)
    try:
        async with run.node_lock(node):
            sender = await _start(
                run,
                template=source.template,
                namespace=source.namespace,
                node=node,
                labels={},
                argument=_sender_argument(attempts),
                finished=True,
            )
            record = await _sender_record(run, sender)
            window_end = (
                0 if record is None else record.end_ms + PRODUCER_CONSTANTS["listener_window_ms"]
            )
            listener = await _listener_past(run, twin, window_end if record else None)
            # The allowed control runs after the denial window, from the same node.
            control = await _start(
                run,
                template="allowed_sender",
                namespace=layout.protected_namespace,
                node=node,
                labels=target.allowed_sender_labels,
                argument=_sender_argument(control_attempts),
                finished=True,
            )
            control_record = await _sender_record(run, control)
        await run.sleep(_POLL_S)
        after = parse_listener_log(await run.cluster.pod_log(twin.namespace, twin.name))
    except ProbeKitRefused:
        return _entry(
            run, entry, verdict=("unavailable", "transport_unavailable"), evidence=evidence
        )
    control_ok = control_record is not None and all(
        after.deliveries(
            nonces=[n for (_a, _p, proto, n, _r) in control_record.attempts if proto == protocol],
            peers=[],
            start_ms=control_record.start_ms,
            end_ms=control_record.end_ms,
        )
        > 0
        for protocol in target.protocols
    )
    sender_addresses = () if sender is None else sender.addresses
    results = [] if record is None else [item[4] for item in record.attempts]
    sampled, covered, moved = (
        (False, False, False) if record is None else listener.window(record.start_ms, window_end)
    )
    verdict = judge_denial(
        DenialEvidence(
            sender_ready=record is not None and "error" not in results,
            listener_ready=listener.ready,
            on_requested_node=sender is not None and sender.node == node,
            family_address=sender is not None and sender.address(family) is not None,
            listener_deliveries=0
            if record is None
            else after.deliveries(
                nonces=nonces,
                peers=sender_addresses,
                start_ms=record.start_ms,
                end_ms=window_end,
                foreign_nonces=run.nonces - set(nonces),
            ),
            sockets_sampled=sampled,
            window_covered=covered,
            overflow_moved=moved,
            client_handshakes=results.count("connected"),
            client_replies=results.count("reply") + results.count("refused"),
            control_succeeded=control_ok,
        ),
        delivery_reason=delivery_reason,
    )
    evidence.update(
        {
            "sender": _pod_evidence(sender),
            "requested_node": node,
            "nonces": nonces,
            "client": _sender_evidence(record),
            "listener_events": [
                list(item)
                for item in after.events
                if item[1] in {str(ipaddress.ip_address(a)) for a in sender_addresses}
                or item[3] in nonces
            ],
            "window": None if record is None else [record.start_ms, window_end],
            "control": {
                "pod": _pod_evidence(control),
                "nonces": control_nonces,
                "client": _sender_evidence(control_record),
                "succeeded": control_ok,
            },
        }
    )
    return _entry(run, entry, verdict=verdict, evidence=evidence)


async def _gather(
    factories: Sequence[Callable[[], Awaitable[ObservationEntry]]],
) -> list[ObservationEntry]:
    gate = asyncio.Semaphore(_CONCURRENCY)

    async def one(factory: Callable[[], Awaitable[ObservationEntry]]) -> ObservationEntry:
        async with gate:
            return await factory()

    return list(await asyncio.gather(*(one(factory) for factory in factories)))


async def _twins(run: _Run) -> dict[str, _Pod] | None:
    layout = run.layout
    twins: dict[str, _Pod] = {}
    for target in layout.targets:
        ports = [target.port, target.closed_port]
        pod = await _start(
            run,
            template="twin",
            namespace=layout.protected_namespace,
            node=layout.eligible_nodes[0],
            labels=target.role_labels,
            argument={"tcp": ports, "udp": ports},
            finished=False,
        )
        if pod is None:
            return None
        twins[target.name] = pod
    return twins


async def case_one(run: _Run, twins: Mapping[str, _Pod]) -> list[ObservationEntry]:
    """Case 1: the broker twin's closed port, from a foreign and an unselected source."""
    broker = run.layout.target("broker")
    return await _gather(
        [
            partial(
                _denial,
                run,
                case=1,
                sub_case="non_vacuity",
                family=family,
                placement=placement,
                source=source,
                twin=twins["broker"],
                target=broker,
                port=broker.closed_port,
                real=False,
                delivery_reason="enforcement_absent",
            )
            for family in run.layout.families
            for placement in PLACEMENTS
            for source in _sources(run.layout, ("foreign_namespace", "protected_unselected"))
        ]
    )


POD_SYNTHESIZING = (
    ("apps", "deployments"),
    ("apps", "replicasets"),
    ("apps", "statefulsets"),
    ("apps", "daemonsets"),
    ("batch", "jobs"),
    ("batch", "cronjobs"),
    ("", "replicationcontrollers"),
)
POLICY_VERBS = ("create", "update", "patch", "delete", "deletecollection")


async def ordinary_identities(run: _Run) -> list[Subject] | None:
    """Every bound subject except the system set and the excluded identities, plus the
    built-in groups every request carries."""
    subjects = set(BUILT_IN_GROUPS)
    for collection in ("rolebindings", "clusterrolebindings"):
        items = await run.cluster.list_items(collection)
        if items is None:
            return None
        for binding in items:
            for subject in binding.get("subjects") or []:
                kind, name = subject.get("kind"), subject.get("name", "")
                if kind not in ("User", "Group", "ServiceAccount") or not name or "*" in name:
                    return None
                namespace = subject.get("namespace", "") if kind == "ServiceAccount" else ""
                if kind == "ServiceAccount" and not namespace:
                    namespace = binding["metadata"].get("namespace", "")
                try:
                    subjects.add(Subject(kind, name, namespace))
                except ProbeKitRefused:
                    # An unresolvable subject leaves the identity set unknown.
                    return None
    exclusions = run.layout.system_set | run.layout.excluded
    return sorted(subjects - exclusions, key=lambda item: (item.kind, item.namespace, item.name))


async def _reviews(
    run: _Run, identities: Sequence[Subject], attributes: Sequence[ResourceAttributes]
) -> tuple[Verdict, list[list[Any]]]:
    gate = asyncio.Semaphore(8)

    async def one(subject: Subject, item: ResourceAttributes) -> bool | None:
        user, groups = subject.review_identity()
        async with gate:
            try:
                return await run.cluster.access_review(user, groups, item)
            except ProbeKitRefused:
                return None

    pairs = [(subject, item) for subject in identities for item in attributes]
    results = await asyncio.gather(*(one(subject, item) for subject, item in pairs))
    record = [
        [
            subject.kind,
            subject.namespace,
            subject.name,
            item.verb,
            item.group,
            item.resource,
            item.subresource,
            item.namespace,
            item.name,
            result,
        ]
        for (subject, item), result in zip(pairs, results, strict=True)
    ]
    return judge_reviews(list(results)), record


def _forged_pod(layout: ProbeLayout, target: Target) -> dict[str, Any]:
    pod = probe_pod(
        template="twin",
        name=f"cni-{layout.campaign}-forged-{target.name}"[:63].rstrip("-"),
        namespace=layout.protected_namespace,
        node=layout.eligible_nodes[0],
        campaign=layout.campaign,
        role_labels=target.role_labels,
        argument={"tcp": [], "udp": []},
    )
    # Not one of the guard-admitted templates: no template label.
    del pod["metadata"]["labels"][TEMPLATE_LABEL]
    return pod


def _combined(verdicts: Sequence[Verdict], clean: Verdict) -> Verdict:
    """The first ``fail``, else the first ``unavailable``, else ``clean``."""
    for outcome in ("fail", "unavailable"):
        found = next((item for item in verdicts if item[0] == outcome), None)
        if found is not None:
            return found
    return clean if verdicts else ("unavailable", "review_unavailable")


async def case_two(
    run: _Run, twins: Mapping[str, _Pod], identities: Sequence[Subject] | None
) -> list[ObservationEntry]:
    layout = run.layout
    entries = await _gather(
        [
            partial(
                _denial,
                run,
                case=2,
                sub_case=target.name,
                family=family,
                placement=placement,
                source=source,
                twin=twins[target.name],
                target=target,
                port=target.port,
                real=True,
                delivery_reason="delivered",
            )
            for target in layout.targets
            for family in layout.families
            for placement in PLACEMENTS
            for source in _sources(
                layout, ("protected_unselected", "foreign_namespace", "release_namespace")
            )
        ]
    )
    # One forgery per target, each with that target's own selected labels; the
    # guard must refuse every one.
    verdicts, forgeries = [], []
    for target in layout.targets:
        try:
            status, message = await run.cluster.dry_run_create_pod(
                layout.protected_namespace, _forged_pod(layout, target)
            )
        except ProbeKitRefused:
            status, message = None, ""
        verdicts.append(judge_guard_dry_run(status, message, layout.guard_names))
        forgeries.append({"target": target.name, "status": status, "message": message[:1024]})
    entries.append(
        _entry(
            run,
            (2, "guard_refusal", *NONE),
            verdict=_combined(verdicts, ("pass", "guard_refused")),
            evidence={"forgeries": forgeries},
        )
    )
    namespace = layout.protected_namespace
    attributes = [
        ResourceAttributes(verb, "", "pods", namespace) for verb in ("create", "update", "patch")
    ]
    attributes += [ResourceAttributes("create", g, r, namespace) for g, r in POD_SYNTHESIZING]
    # Ephemeral containers are added by update or patch of the subresource; all three verbs.
    attributes += [
        ResourceAttributes(verb, "", "pods", namespace, "ephemeralcontainers")
        for verb in ("create", "update", "patch")
    ]
    verdict, record = (
        (("unavailable", "review_unavailable"), [])
        if identities is None
        else await _reviews(run, identities, attributes)
    )
    entries.append(
        _entry(run, (2, "rbac_refusal", *NONE), verdict=verdict, evidence={"reviews": record})
    )
    return entries


def _egress_destinations(
    layout: ProbeLayout, family: str, node_addresses: Mapping[str, tuple[str, ...]]
) -> dict[str, list[tuple[str, int, str]]]:
    def picked(items: Sequence[Destination]) -> list[tuple[str, int, str]]:
        return [
            (d.address, d.port, d.protocol) for d in items if address_family(d.address) == family
        ]

    table: dict[str, list[tuple[str, int, str]]] = {
        "internet": [(a, port, "tcp") for a in INTERNET[family] for port in (443, 80)],
        "external_dns": [
            (a, 53, proto) for a in RESOLVERS[family] for proto in ("dns_udp", "dns_tcp")
        ],
    }
    for sub_case in SUPPLIED_EGRESS:
        table[sub_case] = picked(layout.supplied_egress.get(sub_case, ()))
    for node, addresses in node_addresses.items():
        table["node:" + node] = [
            (a, port, "tcp")
            for a in addresses
            if address_family(a) == family
            for port in (KUBELET_PORT, CLOSED_HIGH_PORT)
        ]
    return table


async def node_addresses(run: _Run) -> dict[str, tuple[str, ...]] | None:
    nodes = await run.cluster.list_items("nodes")
    if nodes is None:
        return None
    found = {}
    for node in nodes:
        name = node["metadata"]["name"]
        if name in run.layout.eligible_nodes:
            found[name] = tuple(
                item["address"]
                for item in node.get("status", {}).get("addresses", [])
                if item.get("type") in ("InternalIP", "ExternalIP")
            )
    return found if set(found) == set(run.layout.eligible_nodes) else None


def _client_counts(results: Sequence[str]) -> tuple[int, int, int]:
    return (
        results.count("connected"),
        results.count("reply") + results.count("refused"),
        results.count("unreachable") + results.count("error"),
    )


async def case_six(run: _Run) -> list[ObservationEntry]:
    """Case 6: one runner-policy pod per family probes every destination, then its peers."""
    addresses = await node_addresses(run)
    entries: list[ObservationEntry] = []
    for family in run.layout.families:
        entries += await _egress_family(run, family, addresses)
    return entries


async def _egress_baseline(
    run: _Run, family: str, node: str, table: Mapping[str, Sequence[tuple[str, int, str]]]
) -> set[tuple[str, int, str]]:
    """Destinations that answer an unrestricted pod in the probe namespace, same node.

    It runs after the runner-policy probe, so the denied direction is asserted
    first. Any handshake, reply or reset counts as reached.
    """
    destinations = sorted({item for items in table.values() for item in items})
    if not destinations:
        return set()
    attempts, _nonces = _attempts(destinations)
    try:
        pod = await _start(
            run,
            template="probe_sender",
            namespace=run.layout.probe_namespace,
            node=node,
            labels={},
            argument=_sender_argument(attempts),
            finished=True,
        )
        record = await _sender_record(run, pod)
    except ProbeKitRefused:
        return set()
    if record is None or pod is None or pod.node != node or pod.address(family) is None:
        return set()
    return {
        (address, port, protocol)
        for address, port, protocol, _nonce, result in record.attempts
        if result in ("connected", "reply", "refused")
    }


async def _egress_family(
    run: _Run, family: str, addresses: Mapping[str, tuple[str, ...]] | None
) -> list[ObservationEntry]:
    layout = run.layout
    entries: list[ObservationEntry] = []
    home = layout.eligible_nodes[0]
    table = _egress_destinations(layout, family, addresses or {})
    denial, nonces_by = [], {}
    for key, destinations in table.items():
        attempts, nonces = _attempts(destinations)
        denial += attempts
        nonces_by[key] = set(nonces)
    peers = [
        (d.address, d.port, d.protocol)
        for d in layout.declared_peers
        if address_family(d.address) == family
    ]
    control, control_nonces = _attempts(peers)
    try:
        pod = await _start(
            run,
            template="runner_policy",
            namespace=layout.protected_namespace,
            node=home,
            labels=layout.runner_labels,
            argument=_sender_argument(denial, control),
            finished=True,
        )
        record = await _sender_record(run, pod)
    except ProbeKitRefused:
        pod, record = None, None
    reached = await _egress_baseline(run, family, home, table)
    observed = [] if record is None else list(record.attempts)
    control_ok = bool(peers) and all(
        any(
            result in ("connected", "reply")
            for (address, port, _p, nonce, result) in observed
            if nonce in control_nonces
            and (address, port) == (str(ipaddress.ip_address(peer[0])), peer[1])
        )
        for peer in peers
    )
    base = {
        "pod": _pod_evidence(pod),
        "requested_node": home,
        "control": {
            "nonces": sorted(control_nonces),
            "client": [list(i) for i in observed if i[3] in control_nonces],
        },
        "baseline_reached": sorted(list(item) for item in reached),
    }

    def verdict_for(keys: Sequence[str]) -> tuple[Verdict, list[list[Any]]]:
        wanted = set().union(*(nonces_by[key] for key in keys))
        if not wanted:
            return ("unavailable", "destination_unknown"), []
        mine = [item for item in observed if item[3] in wanted]
        handshakes, replies, unattributed = _client_counts([item[4] for item in mine])
        evidence = EgressEvidence(
            sender_ready=record is not None,
            on_requested_node=pod is not None and pod.node == home,
            family_address=pod is not None and pod.address(family) is not None,
            handshakes=handshakes,
            replies=replies,
            unattributed=unattributed,
            control_succeeded=control_ok,
            destinations_reached=all(
                (str(ipaddress.ip_address(address)), port, protocol) in reached
                for key in keys
                for address, port, protocol in table[key]
            ),
        )
        return judge_egress(evidence), [list(item) for item in mine]

    for sub_case in (
        "internet",
        "instance_metadata",
        "kubernetes_api",
        "cloud_network",
        "external_dns",
    ):
        verdict, client = verdict_for([sub_case])
        entries.append(
            _entry(
                run,
                (6, sub_case, family, "none", "runner_policy"),
                verdict=verdict,
                evidence=base | {"client": client},
            )
        )
    for placement in PLACEMENTS:
        keys = [
            "node:" + node
            for node in layout.eligible_nodes
            if (node == home) == (placement == "same_node")
        ]
        if addresses is None:
            verdict, client = ("unavailable", "destination_unknown"), []
        elif not keys:
            verdict, client = ("unavailable", "single_eligible_node"), []
        else:
            verdict, client = verdict_for(keys)
        entries.append(
            _entry(
                run,
                (6, "node_addresses", family, placement, "runner_policy"),
                verdict=verdict,
                evidence=base | {"client": client},
            )
        )
    peers_verdict: Verdict = (
        ("pass", "reachable") if control_ok else ("unavailable", "control_failed")
    )
    if record is None:
        peers_verdict = ("unavailable", "phase_timeout")
    entries.append(
        _entry(
            run,
            (6, "declared_peers", family, "none", "runner_policy"),
            verdict=peers_verdict,
            evidence=base,
        )
    )
    return entries


async def policy_snapshot(run: _Run) -> PolicySet | None:
    """NetworkPolicies of the protected namespace and the provider policy objects."""
    layout = run.layout
    found: set[tuple[str, ...]] = set()
    collections = (
        "networkpolicies:" + layout.protected_namespace,
        *layout.provider_policy_collections,
    )
    for collection in collections:
        items = await run.cluster.list_items(collection)
        if items is None:
            return None
        for item in items:
            meta = item["metadata"]
            namespace = meta.get("namespace", "")
            if namespace not in ("", layout.protected_namespace):
                continue
            found.add((collection, namespace, meta["name"], meta["uid"], meta["resourceVersion"]))
    return frozenset(found)


def _provider_resource(collection: str) -> tuple[str, str]:
    _empty, _apis, group, _version, resource = collection.split("/")
    return group, resource


async def case_eight_reviews(run: _Run, identities: Sequence[Subject] | None) -> ObservationEntry:
    layout = run.layout
    namespace = layout.protected_namespace
    attributes = [
        ResourceAttributes(verb, "networking.k8s.io", "networkpolicies", namespace)
        for verb in POLICY_VERBS
    ]
    for collection in layout.provider_policy_collections:
        group, resource = _provider_resource(collection)
        for scope in (namespace, ""):
            attributes += [
                ResourceAttributes(verb, group, resource, scope) for verb in POLICY_VERBS
            ]
    for ref in layout.policy_agent_objects:
        attributes += [
            ResourceAttributes(verb, ref.group, ref.resource, ref.namespace, "", ref.name)
            for verb in POLICY_VERBS
        ]
    for name in (namespace, layout.probe_namespace, layout.foreign_namespace):
        attributes += [
            ResourceAttributes(verb, "", "namespaces", "", "", name) for verb in ("update", "patch")
        ]
    verdict, record = (
        (("unavailable", "review_unavailable"), [])
        if identities is None
        else await _reviews(run, identities, attributes)
    )
    return _entry(run, (8, "rbac_policy", *NONE), verdict=verdict, evidence={"reviews": record})


async def _cleanup(run: _Run) -> list[str]:
    """Delete every pod this run created, by UID, and confirm absence."""
    leftovers = []
    for namespace, name, uid in run.created:
        try:
            await run.cluster.delete_pod(namespace, name, uid)
        except ProbeKitRefused:
            pass
    deadline = run.clock() + 120
    for namespace, name, uid in run.created:
        while True:
            try:
                pod = await run.cluster.read_pod(namespace, name)
            except ProbeKitRefused:
                pod = {"metadata": {"uid": uid}}
            if pod is None or pod["metadata"]["uid"] != uid:
                break
            if run.clock() > deadline:
                leftovers.append(f"{namespace}/{name}/{uid}")
                break
            await run.sleep(_POLL_S)
    return leftovers


@dataclass(frozen=True, slots=True)
class KitResult:
    measurement: Measurement
    evidence: tuple[dict[str, Any], ...]
    leftovers: tuple[str, ...]


class ProbeKit:
    """Runs cases 1, 2, 6 and 8 in clause 4 order. Implements ``BehavioralNetworkProducer``."""

    producer_kind = PRODUCER_KIND

    def __init__(
        self,
        cluster: ProbeCluster,
        layout: ProbeLayout,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._cluster, self._layout, self._clock, self._sleep = cluster, layout, clock, sleep
        self.last: KitResult | None = None

    async def measure(self) -> Measurement:
        run = _Run(self._cluster, self._layout, self._clock, self._sleep)
        try:
            measurement = await self._measure(run)
        finally:
            leftovers = await _cleanup(run)
        if leftovers:
            # Unconfirmed absence blocks acceptance; the evidence names each pod.
            measurement = Measurement(
                measurement.entries, measurement.refusal or "cleanup_unconfirmed"
            )
        run.evidence.append({"probe_kit_digest": probe_kit_digest(), "leftovers": leftovers})
        self.last = KitResult(measurement, tuple(run.evidence), tuple(leftovers))
        return measurement

    async def _measure(self, run: _Run) -> Measurement:
        snapshots = [await policy_snapshot(run)]
        try:
            twins = await _twins(run)
        except ProbeKitRefused:
            twins = None
        if twins is None:
            return Measurement((), "non_vacuity_unavailable")
        entries = await case_one(run, twins)
        snapshots.append(await policy_snapshot(run))
        if any(item.outcome != "pass" for item in entries):
            failed = any(item.outcome == "fail" for item in entries)
            return Measurement(
                tuple(entries), "enforcement_absent" if failed else "non_vacuity_unavailable"
            )
        identities = await ordinary_identities(run)
        entries += await case_two(run, twins, identities)
        snapshots.append(await policy_snapshot(run))
        entries += await case_six(run)
        snapshots.append(await policy_snapshot(run))
        entries.append(await case_eight_reviews(run, identities))
        snapshots.append(await policy_snapshot(run))
        verdict = judge_policy_sets(snapshots)
        entries.append(
            _entry(
                run,
                (8, "all_policies_present", *NONE),
                verdict=verdict,
                evidence={"snapshots": [None if s is None else sorted(s) for s in snapshots]},
            )
        )
        refused = next((item for item in entries if item.outcome != "pass"), None)
        return Measurement(tuple(entries), None if refused is None else refused.reason)


def required_entries(layout: ProbeLayout) -> frozenset[EntryKey]:
    """The entries of cases 1, 2, 6 and 8 that must all be ``pass`` (clause 4)."""
    keys: set[EntryKey] = set()
    for family in layout.families:
        for placement in PLACEMENTS:
            for source in ("foreign_namespace", "protected_unselected"):
                keys.add((1, "non_vacuity", family, placement, source))
            for target in ("broker", "mediator"):
                for source in ("protected_unselected", "foreign_namespace", "release_namespace"):
                    keys.add((2, target, family, placement, source))
            keys.add((6, "node_addresses", family, placement, "runner_policy"))
        for sub_case in (
            "internet",
            "instance_metadata",
            "kubernetes_api",
            "cloud_network",
            "external_dns",
            "declared_peers",
        ):
            keys.add((6, sub_case, family, "none", "runner_policy"))
    for case, sub_case in (
        (2, "guard_refusal"),
        (2, "rbac_refusal"),
        (8, "rbac_policy"),
        (8, "all_policies_present"),
    ):
        keys.add((case, sub_case, *NONE))
    return frozenset(keys)
