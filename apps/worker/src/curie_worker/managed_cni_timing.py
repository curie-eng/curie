"""Timing-sensitive cases 3, 4, 5 and 7 of the managed CNI behavioral producer, ADR 0238.

@spec MANAGED-CNI-4. These cases build on the merged probe kit (cases 1, 2, 6, 8
in :mod:`managed_cni_cases`) and reuse its ``_Run``, pod lifecycle, denial probe
and review helpers; they do not fork them. Every verdict is fail closed: a case a
local cluster cannot provoke is ``unavailable`` (for example an unprovoked
address reuse, or an IPv6 source a raw socket cannot forge), never a ``pass``.

- Case 3, address spoofing: a raw-socket sender forges an allowed source address
  and the owned twin must record nothing.
- Case 4, first-packet denial and address reuse: an unselected pod sends from its
  first instant, repeated across a bounded window; a reused address must also be
  refused.
- Case 5, retained-connection refusal: a flow allowed when established must carry
  no data after the workload is replaced, or after a probe-namespace policy change
  removes the allowance.
- Case 7, node-local traffic: kubelet reaches a twin from the node, and, when it
  does, effective RBAC and admission must refuse every node-capable right to
  ordinary identities.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
from collections.abc import Mapping, Sequence
from typing import Any

from .managed_cni_cases import (
    POD_SYNTHESIZING,
    ProbeLayout,
    ResourceAttributes,
    Subject,
    Target,
    _attempts,
    _entry,
    _listener_past,
    _nonce,
    _other_node,
    _Pod,
    _pod_evidence,
    _reviews,
    _Run,
    _sender_argument,
    _sender_evidence,
    _sender_record,
    _Source,
    node_addresses,
)
from .managed_cni_probe_kit import (
    PLACEMENTS,
    PRODUCER_CONSTANTS,
    ObservationEntry,
    ProbeKitRefused,
    RetainedEvidence,
    SpoofEvidence,
    Verdict,
    address_family,
    judge_guard_dry_run,
    judge_node_origin,
    judge_retained,
    judge_running_nodes,
    judge_spoof,
)
from .managed_cni_probe_programs import (
    parse_listener_log,
    parse_retained_sender_log,
    parse_spoof_log,
    probe_pod,
)

EntryKey = tuple[int, str, str, str, str]
NONE = ("none", "none", "none")

# Case 7 node-capable pod fields an eligible-node pod may not have (clause 6/7).
NODE_CAPABLE = (
    ("hostNetwork", True),
    ("hostPID", True),
    ("hostIPC", True),
    ("hostPath", {"volumes": [{"name": "h", "hostPath": {"path": "/"}}]}),
    ("privileged", {"containers": True}),
)


def _canonical(address: str) -> str:
    return str(ipaddress.ip_address(address))


async def _listener_deliveries(
    run: _Run,
    twin: _Pod,
    *,
    nonces: Sequence[str],
    peers: Sequence[str],
    start_ms: int,
    end_ms: int,
) -> tuple[int, ListenerWindow]:
    record = await _listener_past(run, twin, end_ms)
    count = record.deliveries(
        nonces=list(nonces),
        peers=list(peers),
        start_ms=start_ms,
        end_ms=end_ms,
        foreign_nonces=run.nonces - set(nonces),
    )
    sampled, covered, moved = record.window(start_ms, end_ms)
    return count, ListenerWindow(record.ready, sampled, covered, moved)


class ListenerWindow:
    __slots__ = ("ready", "sampled", "covered", "moved")

    def __init__(self, ready: bool, sampled: bool, covered: bool, moved: bool) -> None:
        self.ready, self.sampled, self.covered, self.moved = ready, sampled, covered, moved


async def _allowed_control(run: _Run, target: Target, twin: _Pod, node: str, family: str) -> bool:
    """An allowed-sender probe to the twin on ``node`` succeeds, so the twin is live."""
    twin_address = twin.address(family)
    if twin_address is None:
        return False
    attempts, nonces = _attempts(
        [(twin_address, target.port, protocol) for protocol in target.protocols]
    )
    run.nonces.update(nonces)
    control = await _start_sender(
        run,
        template="allowed_sender",
        namespace=run.layout.protected_namespace,
        node=node,
        labels=target.allowed_sender_labels,
        argument=_sender_argument(attempts),
    )
    record = await _sender_record(run, control)
    if record is None:
        return False
    after = parse_listener_log(await run.cluster.pod_log(twin.namespace, twin.name))
    return all(
        after.deliveries(
            nonces=[n for (_a, _p, proto, n, _r) in record.attempts if proto == protocol],
            peers=[],
            start_ms=record.start_ms,
            end_ms=record.end_ms,
        )
        > 0
        for protocol in target.protocols
    )


async def _start_sender(
    run: _Run,
    *,
    template: str,
    namespace: str,
    node: str,
    labels: Mapping[str, str],
    argument: Mapping[str, Any],
) -> _Pod | None:
    from .managed_cni_cases import _start

    return await _start(
        run,
        template=template,
        namespace=namespace,
        node=node,
        labels=labels,
        argument=argument,
        finished=True,
    )


async def _start_listener(
    run: _Run,
    *,
    template: str,
    namespace: str,
    node: str,
    labels: Mapping[str, str],
    argument: Mapping[str, Any],
) -> _Pod | None:
    from .managed_cni_cases import _start

    return await _start(
        run,
        template=template,
        namespace=namespace,
        node=node,
        labels=labels,
        argument=argument,
        finished=False,
    )


async def _terminate(run: _Run, pod: _Pod) -> bool:
    """Delete one pod by UID and confirm its absence."""
    try:
        await run.cluster.delete_pod(pod.namespace, pod.name, pod.uid)
    except ProbeKitRefused:
        return False
    deadline = run.clock() + 60
    while run.clock() < deadline:
        try:
            current = await run.cluster.read_pod(pod.namespace, pod.name)
        except ProbeKitRefused:
            return False
        if current is None or current["metadata"]["uid"] != pod.uid:
            return True
        await run.sleep(1.0)
    return False


async def _address_in_use(run: _Run, address: str) -> bool | None:
    """Whether any current pod holds ``address`` (clause 4: reuse must not fool us)."""
    pods = await run.cluster.list_items("pods")
    if pods is None:
        return None
    wanted = _canonical(address)
    for pod in pods:
        for item in pod.get("status", {}).get("podIPs", []):
            if item.get("ip") and _canonical(item["ip"]) == wanted:
                return True
    return False


# --- Case 3: address spoofing ---


async def case_three(run: _Run, twins: Mapping[str, _Pod]) -> list[ObservationEntry]:
    entries: list[ObservationEntry] = []
    for family in run.layout.families:
        for placement in PLACEMENTS:
            for sub_case in ("spoof_live", "spoof_terminated"):
                entries.append(
                    await _spoof(run, twins=twins, sub_case=sub_case, family=family,
                                 placement=placement)
                )
    return entries


async def _spoof(
    run: _Run, *, twins: Mapping[str, _Pod], sub_case: str, family: str, placement: str
) -> ObservationEntry:
    """One case-3 entry: every target twin (broker and mediator) must record nothing
    from the forged source. The entry key has no target dimension, so the broker and
    mediator results are combined (clause 4, case 3)."""
    key: EntryKey = (3, sub_case, family, placement, "raw_socket")
    verdicts: list[Verdict] = []
    records = []
    for target in run.layout.targets:
        verdict, record = await _spoof_target(
            run, target=target, twin=twins[target.name], sub_case=sub_case, family=family,
            placement=placement,
        )
        verdicts.append(verdict)
        records.append(record)
    return _entry(run, key, verdict=_combined(verdicts, ("pass", "denied")),
                  evidence={"targets": records})


async def _spoof_target(
    run: _Run, *, target: Target, twin: _Pod, sub_case: str, family: str, placement: str
) -> tuple[Verdict, dict[str, Any]]:
    layout = run.layout
    node = twin.node if placement == "same_node" else _other_node(layout, twin.node)
    record: dict[str, Any] = {"target": target.name, "listener": _pod_evidence(twin)}
    if node is None:
        return ("unavailable", "single_eligible_node"), record
    twin_address = twin.address(family)
    if twin_address is None:
        return ("unavailable", "no_family_address"), record
    if family != "ipv4":
        # NET_RAW alone cannot forge an IPv6 source without L2 crafting (clause 2);
        # widening the capability would be an ADR 0238 revision, so this refuses.
        return ("unavailable", "transport_unavailable"), record
    allowed = await _start_listener(
        run, template="twin", namespace=layout.protected_namespace, node=node,
        labels=target.allowed_sender_labels, argument={"tcp": [target.port], "udp": [target.port]},
    )
    if allowed is None or allowed.address(family) is None:
        return ("unavailable", "phase_timeout"), record
    forged = allowed.address(family)
    assert forged is not None
    reassigned = False
    absent_ms: int | None = None
    if sub_case == "spoof_terminated":
        if not await _terminate(run, allowed):
            return ("unavailable", "phase_timeout"), record
        absent_ms = int(run.clock() * 1000)
        in_use = await _address_in_use(run, forged)
        reassigned = in_use is not False  # None (unknown) or True both refuse the probe
    nonces = [_nonce() for _ in target.protocols]
    run.nonces.update(nonces)
    packets = [
        {"src": forged, "addr": twin_address, "port": target.port, "proto": protocol, "nonce": n}
        for protocol, n in zip(target.protocols, nonces, strict=True)
    ]
    try:
        async with run.node_lock(node):
            start_ms = int(run.clock() * 1000)
            # Clause 4: the forged address must have been confirmed absent under 30 s
            # earlier, or it no longer measures a recently terminated address.
            if absent_ms is not None and start_ms - absent_ms > 30_000:
                record["stale_ms"] = start_ms - absent_ms
                return ("unavailable", "address_reassigned"), record
            sender = await _start_sender(
                run, template="spoofing", namespace=layout.probe_namespace, node=node, labels={},
                argument={"packets": packets,
                          "repeat": PRODUCER_CONSTANTS["denial_tcp_attempts"],
                          "interval_ms": PRODUCER_CONSTANTS["attempt_interval_ms"]},
            )
            spoof = None if sender is None else parse_spoof_log(
                await run.cluster.pod_log(sender.namespace, sender.name)
            )
            end_ms = int(run.clock() * 1000) + PRODUCER_CONSTANTS["listener_window_ms"]
            deliveries, window = await _listener_deliveries(
                run, twin, nonces=nonces, peers=[forged], start_ms=start_ms, end_ms=end_ms
            )
            control_ok = await _allowed_control(run, target, twin, node, family)
    except ProbeKitRefused:
        return ("unavailable", "transport_unavailable"), record
    verdict = judge_spoof(
        SpoofEvidence(
            sender_ready=spoof is not None and spoof.sent,
            listener_ready=window.ready,
            on_requested_node=sender is not None and sender.node == node,
            forged_family_address=True,
            listener_deliveries=deliveries,
            sockets_sampled=window.sampled,
            window_covered=window.covered,
            overflow_moved=window.moved,
            control_succeeded=control_ok,
            address_reassigned=reassigned,
        )
    )
    record.update({"forged_source": forged, "sender": _pod_evidence(sender),
                   "requested_node": node, "deliveries": deliveries, "reassigned": reassigned,
                   "control_succeeded": control_ok})
    return verdict, record


# --- Case 4: first-packet denial and address reuse ---


async def case_four(run: _Run, twins: Mapping[str, _Pod]) -> list[ObservationEntry]:
    broker = run.layout.target("broker")
    entries: list[ObservationEntry] = []
    for family in run.layout.families:
        for placement in PLACEMENTS:
            for source in _sources(run.layout, ("protected_unselected", "foreign_namespace")):
                entries.append(
                    await _first_packet(
                        run, twin=twins["broker"], broker=broker, family=family,
                        placement=placement, source=source,
                    )
                )
        # Address reuse is inherently same-node: an unselected pod must take over a
        # terminated allowed sender's address on its node.
        entries.append(
            await _address_reuse(run, twin=twins["broker"], broker=broker, family=family)
        )
    return entries


def _sources(layout: ProbeLayout, kinds: Sequence[str]) -> list[_Source]:
    table = {
        "foreign_namespace": _Source("foreign_namespace", "probe_sender", layout.foreign_namespace),
        "protected_unselected": _Source(
            "protected_unselected", "protected_probe", layout.protected_namespace
        ),
    }
    return [table[kind] for kind in kinds]


async def _first_packet(
    run: _Run, *, twin: _Pod, broker: Target, family: str, placement: str, source: _Source
) -> ObservationEntry:
    layout = run.layout
    key: EntryKey = (4, "new_pod", family, placement, source.kind)
    node = twin.node if placement == "same_node" else _other_node(layout, twin.node)
    evidence: dict[str, Any] = {"listener": _pod_evidence(twin), "source": source.kind}
    if node is None:
        return _entry(run, key, verdict=("unavailable", "single_eligible_node"), evidence=evidence)
    twin_address = twin.address(family)
    if twin_address is None:
        return _entry(run, key, verdict=("unavailable", "no_family_address"), evidence=evidence)
    reps = PRODUCER_CONSTANTS["first_packet_repetitions"]
    start_ms = int(run.clock() * 1000)
    all_nonces: list[str] = []
    senders: list[_Pod | None] = []
    results: list[str] = []
    gate = asyncio.Semaphore(4)

    async def one_rep() -> None:
        # Each repetition is a fresh unselected pod that sends from its first
        # instant. Attribution is by the repetition's own nonces, so repetitions
        # may run concurrently without a per-node address lock.
        attempts, nonces = _attempts([(twin_address, broker.port, p) for p in broker.protocols])
        run.nonces.update(nonces)
        all_nonces.extend(nonces)
        async with gate:
            sender = await _start_sender(
                run,
                template=source.template,
                namespace=source.namespace,
                node=node,
                labels={},
                # Attributed by its own nonces, so repetitions may share a node.
                argument=_sender_argument(attempts) | {"nonce_only": True},
            )
        record = await _sender_record(run, sender)
        senders.append(sender)
        if record is not None:
            results.extend(item[4] for item in record.attempts)

    await asyncio.gather(*(one_rep() for _ in range(reps)))
    end_ms = int(run.clock() * 1000) + PRODUCER_CONSTANTS["listener_window_ms"]
    deliveries, window = await _listener_deliveries(
        run, twin, nonces=all_nonces, peers=[], start_ms=start_ms, end_ms=end_ms
    )
    control_ok = await _allowed_control(run, broker, twin, node, family)
    from .managed_cni_probe_kit import DenialEvidence, judge_denial

    verdict = judge_denial(
        DenialEvidence(
            sender_ready=bool(senders) and all(s is not None for s in senders)
            and "error" not in results,
            listener_ready=window.ready,
            on_requested_node=all(s is not None and s.node == node for s in senders),
            family_address=all(s is not None and s.address(family) is not None for s in senders),
            listener_deliveries=deliveries,
            sockets_sampled=window.sampled,
            window_covered=window.covered,
            overflow_moved=window.moved,
            client_handshakes=results.count("connected"),
            client_replies=results.count("reply") + results.count("refused"),
            control_succeeded=control_ok,
        ),
        delivery_reason="admitted_before_propagation",
    )
    evidence.update(
        {"repetitions": reps, "requested_node": node, "deliveries": deliveries,
         "control_succeeded": control_ok}
    )
    return _entry(run, key, verdict=verdict, evidence=evidence)


async def _address_reuse(
    run: _Run, *, twin: _Pod, broker: Target, family: str
) -> ObservationEntry:
    layout = run.layout
    key: EntryKey = (4, "address_reuse", family, "same_node", "protected_unselected")
    node = twin.node
    evidence: dict[str, Any] = {"listener": _pod_evidence(twin)}
    twin_address = twin.address(family)
    if twin_address is None:
        return _entry(run, key, verdict=("unavailable", "no_family_address"), evidence=evidence)
    allowed = await _start_listener(
        run,
        template="twin",
        namespace=layout.protected_namespace,
        node=node,
        labels=broker.allowed_sender_labels,
        argument={"tcp": [broker.port], "udp": [broker.port]},
    )
    target_ip = None if allowed is None else allowed.address(family)
    if allowed is None or target_ip is None or not await _terminate(run, allowed):
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    max_pods = PRODUCER_CONSTANTS["address_reuse_max_pods"]
    deadline = run.clock() + PRODUCER_CONSTANTS["address_reuse_max_ms"] / 1000
    reuser: _Pod | None = None
    nonces: list[str] = []
    start_ms = int(run.clock() * 1000)
    for _ in range(max_pods):
        if run.clock() > deadline:
            break
        attempts, probe_nonces = _attempts(
            [(twin_address, broker.port, p) for p in broker.protocols]
        )
        candidate = await _start_sender(
            run,
            template="protected_probe",
            namespace=layout.protected_namespace,
            node=node,
            labels={},
            argument=_sender_argument(attempts) | {"nonce_only": True},
        )
        if candidate is not None and candidate.address(family) == target_ip:
            reuser, nonces = candidate, probe_nonces
            run.nonces.update(probe_nonces)
            break
    if reuser is None:
        evidence["reused_address"] = target_ip
        return _entry(run, key, verdict=("unavailable", "reuse_not_provoked"), evidence=evidence)
    end_ms = int(run.clock() * 1000) + PRODUCER_CONSTANTS["listener_window_ms"]
    deliveries, window = await _listener_deliveries(
        run, twin, nonces=nonces, peers=[target_ip], start_ms=start_ms, end_ms=end_ms
    )
    control_ok = await _allowed_control(run, broker, twin, node, family)
    from .managed_cni_probe_kit import DenialEvidence, judge_denial

    # The reusing pod's own client results close the client-side path too: a
    # completed handshake or a reply is a fail, not just a listener delivery.
    record = await _sender_record(run, reuser)
    results = [] if record is None else [item[4] for item in record.attempts]
    verdict = judge_denial(
        DenialEvidence(
            sender_ready=record is not None and "error" not in results,
            listener_ready=window.ready,
            on_requested_node=reuser.node == node,
            family_address=reuser.address(family) is not None,
            listener_deliveries=deliveries,
            sockets_sampled=window.sampled,
            window_covered=window.covered,
            overflow_moved=window.moved,
            client_handshakes=results.count("connected"),
            client_replies=results.count("reply") + results.count("refused"),
            control_succeeded=control_ok,
        ),
        delivery_reason="admitted_before_propagation",
    )
    evidence.update({"reused_address": target_ip, "reuser": _pod_evidence(reuser),
                     "client": _sender_evidence(record), "deliveries": deliveries,
                     "control_succeeded": control_ok})
    return _entry(run, key, verdict=verdict, evidence=evidence)


# --- Case 5: retained-connection refusal ---

# The role label a probe policy selects on. Probe policies live only in probe-a.
_ROLE = "curietech.ai/managed-cni-probe-role"
_PROBE_POLICY = "curietech.ai/managed-cni-probe-policy"
_PRE_CHANGE_HEARTBEATS = 5


async def case_five(run: _Run, twins: Mapping[str, _Pod]) -> list[ObservationEntry]:
    entries: list[ObservationEntry] = []
    broker = run.layout.target("broker")
    for family in run.layout.families:
        for placement in PLACEMENTS:
            entries.append(await _policy_changed(run, broker=broker, family=family,
                                                 placement=placement))
            entries.append(await _workload_replaced(run, twin=twins["broker"], broker=broker,
                                                    family=family, placement=placement))
    return entries


def _probe_policy(name: str, namespace: str, listener_role: str, sender_role: str | None,
                  port: int) -> dict[str, Any]:
    ingress = (
        []
        if sender_role is None
        else [
            {
                "from": [{"podSelector": {"matchLabels": {_ROLE: sender_role}}}],
                "ports": [{"protocol": "TCP", "port": port}],
            }
        ]
    )
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name, "namespace": namespace, "labels": {_PROBE_POLICY: "1"}},
        "spec": {
            "podSelector": {"matchLabels": {_ROLE: listener_role}},
            "policyTypes": ["Ingress"],
            "ingress": ingress,
        },
    }


async def _create_policy(run: _Run, name: str, document: dict[str, Any]) -> None:
    await run.cluster.create_network_policy(run.layout.probe_namespace, document)
    run.created_policies.append((run.layout.probe_namespace, name))


async def _delete_policy(run: _Run, name: str) -> None:
    try:
        await run.cluster.delete_network_policy(run.layout.probe_namespace, name)
    except ProbeKitRefused:
        pass


async def _launch(
    run: _Run, *, template: str, namespace: str, node: str, labels: Mapping[str, str],
    argument: Mapping[str, Any],
) -> tuple[str, str] | None:
    """Create a probe pod without waiting for a phase; return (name, uid)."""
    import secrets

    run.counter += 1
    name = f"cni-{run.layout.campaign}-{run.counter}-{secrets.token_hex(3)}"[:63].rstrip("-")
    document = probe_pod(
        template=template, name=name, namespace=namespace, node=node,
        campaign=run.layout.campaign, role_labels=labels, argument=argument,
    )
    try:
        created = await run.cluster.create_pod(namespace, document)
    except ProbeKitRefused:
        return None
    uid = created["metadata"]["uid"]
    run.created.append((namespace, name, uid))
    return name, uid


async def _await_log(
    run: _Run, namespace: str, name: str, uid: str, needle: str, deadline: float
) -> bool:
    while run.clock() < deadline:
        try:
            pod = await run.cluster.read_pod(namespace, name)
            if pod is None or pod["metadata"]["uid"] != uid:
                return False
            if needle in await run.cluster.pod_log(namespace, name):
                return True
        except ProbeKitRefused:
            return False
        await run.sleep(1.0)
    return False


async def _await_terminal(run: _Run, namespace: str, name: str, uid: str, deadline: float) -> bool:
    while run.clock() < deadline:
        try:
            pod = await run.cluster.read_pod(namespace, name)
        except ProbeKitRefused:
            return False
        if pod is None or pod["metadata"]["uid"] != uid:
            return False
        if pod.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            return True
        await run.sleep(1.0)
    return False


async def _policy_changed(
    run: _Run, *, broker: Target, family: str, placement: str
) -> ObservationEntry:
    layout = run.layout
    key: EntryKey = (5, "policy_changed", family, placement, "retained_peer")
    node = layout.eligible_nodes[0]
    other = _other_node(layout, node)
    evidence: dict[str, Any] = {}
    if placement == "cross_node" and other is None:
        return _entry(run, key, verdict=("unavailable", "single_eligible_node"), evidence=evidence)
    sender_node = node if placement == "same_node" else other
    assert sender_node is not None
    port = broker.port
    allow = f"cni-{layout.campaign}-ra-{family[-1]}-{placement[0]}"[:63].rstrip("-")
    deny = f"cni-{layout.campaign}-rd-{family[-1]}-{placement[0]}"[:63].rstrip("-")
    ctl = f"cni-{layout.campaign}-rc-{family[-1]}-{placement[0]}"[:63].rstrip("-")
    change_ms: int | None = None
    sender_log = ""
    listener_log = ""
    sender_node_ok = False
    liveness_ok = False
    probe_ns = layout.probe_namespace
    try:
        await _create_policy(run, allow, _probe_policy(allow, probe_ns, "ret-listener",
                                                       "ret-sender", port))
        listener = await _start_listener(
            run, template="probe_listener", namespace=probe_ns, node=node,
            labels={_ROLE: "ret-listener"}, argument={"tcp": [port], "udp": [port], "hold": True},
        )
        listener_ip = None if listener is None else listener.address(family)
        if listener is None or listener_ip is None:
            return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
        nonces = _heartbeat_nonces()
        run.nonces.update(nonces)
        launched = await _launch(
            run, template="retained_sender", namespace=probe_ns, node=sender_node,
            labels={_ROLE: "ret-sender"},
            argument={"addr": listener_ip, "port": port, "nonces": nonces,
                      "interval_ms": PRODUCER_CONSTANTS["retained_heartbeat_ms"],
                      "timeout_ms": PRODUCER_CONSTANTS["connect_timeout_ms"]},
        )
        if launched is None:
            return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
        s_name, s_uid = launched
        deadline = run.clock() + PRODUCER_CONSTANTS["pod_phase_timeout_ms"] / 1000
        if not await _await_log(run, probe_ns, s_name, s_uid, "established", deadline):
            return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
        # A few heartbeats flow while still allowed, then remove the allowance.
        await run.sleep(_PRE_CHANGE_HEARTBEATS * PRODUCER_CONSTANTS["retained_heartbeat_ms"] / 1000)
        change_ms = int(run.clock() * 1000)
        await _delete_policy(run, allow)
        await _create_policy(run, deny, _probe_policy(deny, probe_ns, "ret-listener", None, port))
        term_deadline = run.clock() + PRODUCER_CONSTANTS["pod_phase_timeout_ms"] / 1000
        await _await_terminal(run, probe_ns, s_name, s_uid, term_deadline)
        sender_log = await run.cluster.pod_log(probe_ns, s_name)
        sender_pod = await run.cluster.read_pod(probe_ns, s_name)
        sender_node_ok = (
            sender_pod is not None and sender_pod["spec"].get("nodeName") == sender_node
        )
        await run.sleep(PRODUCER_CONSTANTS["listener_window_ms"] / 1000)
        listener_log = await run.cluster.pod_log(listener.namespace, listener.name)
        # Post-change listener-liveness control: a separately-allowed probe must
        # still reach the listener, so a pass is never from a dead listener.
        liveness_ok = await _listener_liveness(run, listener, listener_ip, port, node, ctl)
    except ProbeKitRefused:
        return _entry(run, key, verdict=("unavailable", "transport_unavailable"), evidence=evidence)
    finally:
        for name in (allow, deny, ctl):
            await _delete_policy(run, name)
    try:
        sender = parse_retained_sender_log(sender_log)
        listener_record = parse_listener_log(listener_log)
    except ProbeKitRefused:
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    bound = PRODUCER_CONSTANTS["retained_bound_ms"]
    received = {nonce for _k, _p, _port, nonce, _at in listener_record.events}
    pre_ok = [n for (n, at, r) in sender.sent if change_ms is not None and at <= change_ms
              and r == "ok"]
    post_ok = [n for (n, at, r) in sender.sent if change_ms is not None and at > change_ms + bound
               and r == "ok"]
    post_delivered = sum(1 for n in post_ok if n in received)
    window_start = change_ms or 0
    window_end = window_start + bound + PRODUCER_CONSTANTS["listener_window_ms"]
    sampled, covered, moved = listener_record.window(window_start, window_end)
    evidence.update({"change_ms": change_ms, "pre_delivered": any(n in received for n in pre_ok),
                     "post_sent": len(post_ok), "post_delivered": post_delivered,
                     "liveness_ok": liveness_ok})
    if not post_ok and post_delivered == 0:
        # The sender sent no heartbeat past the bound, so retention cannot be judged.
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    verdict = judge_retained(
        RetainedEvidence(
            established=sender.connected and change_ms is not None
            and any(n in received for n in pre_ok),
            listener_ready=listener_record.ready,
            on_requested_node=sender_node_ok,
            family_address=sender.local_address is not None,
            post_trigger_deliveries=post_delivered,
            sockets_sampled=sampled,
            window_covered=covered,
            overflow_moved=moved,
            trigger_confirmed=change_ms is not None,
            control_succeeded=liveness_ok,
            reuse_provoked=True,
        )
    )
    return _entry(run, key, verdict=verdict, evidence=evidence)


async def _listener_liveness(
    run: _Run, listener: _Pod, listener_ip: str, port: int, node: str, policy_name: str
) -> bool:
    """A separately-allowed probe reaches the hold listener after the change."""
    await _create_policy(
        run, policy_name,
        _probe_policy(policy_name, run.layout.probe_namespace, "ret-listener", "ret-control", port),
    )
    attempts, nonces = _attempts([(listener_ip, port, "tcp")])
    run.nonces.update(nonces)
    sender = await _start_sender(
        run, template="probe_sender", namespace=run.layout.probe_namespace, node=node,
        labels={_ROLE: "ret-control"}, argument=_sender_argument(attempts) | {"nonce_only": True},
    )
    record = await _sender_record(run, sender)
    if record is None:
        return False
    after = parse_listener_log(await run.cluster.pod_log(listener.namespace, listener.name))
    return after.deliveries(
        nonces=nonces, peers=[], start_ms=record.start_ms, end_ms=record.end_ms
    ) > 0


def _heartbeat_nonces() -> list[str]:
    interval = PRODUCER_CONSTANTS["retained_heartbeat_ms"]
    span = PRODUCER_CONSTANTS["retained_bound_ms"] + 2 * PRODUCER_CONSTANTS["listener_window_ms"]
    return [_nonce() for _ in range(_PRE_CHANGE_HEARTBEATS + span // interval + 1)]


def _established_endpoint(log: str) -> tuple[str, int] | None:
    """The retained sender's (local address, local port) from its ``established`` line."""
    for line in log.splitlines():
        if not line.startswith("{"):
            continue
        try:
            document = json.loads(line)
        except ValueError:
            return None
        if document.get("ev") == "established" and document.get("laddr") and document.get("lport"):
            return str(document["laddr"]), int(document["lport"])
    return None


async def _workload_replaced(
    run: _Run, *, twin: _Pod, broker: Target, family: str, placement: str
) -> ObservationEntry:
    """Establish a real retained connection from an allowed-sender twin, replace the
    sender, provoke reuse of its address, then inject into the original four-tuple
    from the spoofing template. If connection tracking lets the flow resume, the
    hold twin receives data from the reused address and the case fails.

    Unprovoked reuse within the bound is ``unavailable`` (``reuse_not_provoked``),
    the explicit ADR 0238 outcome; it is never a pass. This is distinct from case 4:
    it resumes the original four-tuple and sequence state, not a fresh connection.
    """
    layout = run.layout
    key: EntryKey = (5, "workload_replaced", family, placement, "retained_peer")
    node = twin.node if placement == "same_node" else _other_node(layout, twin.node)
    evidence: dict[str, Any] = {"listener": _pod_evidence(twin)}
    if node is None:
        return _entry(run, key, verdict=("unavailable", "single_eligible_node"), evidence=evidence)
    if family != "ipv4":
        # The four-tuple injection needs the IPv4 raw-socket path (clause 2).
        return _entry(run, key, verdict=("unavailable", "transport_unavailable"), evidence=evidence)
    hold = await _start_listener(
        run, template="twin", namespace=layout.protected_namespace, node=twin.node,
        labels=broker.role_labels, argument={"tcp": [broker.port], "udp": [broker.port],
                                             "hold": True},
    )
    hold_ip = None if hold is None else hold.address(family)
    if hold is None or hold_ip is None:
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    # Establish a real retained TCP flow from an allowed-sender twin, then replace it.
    launched = await _launch(
        run, template="retained_sender", namespace=layout.protected_namespace, node=node,
        labels=broker.allowed_sender_labels,
        argument={"addr": hold_ip, "port": broker.port, "nonces": _heartbeat_nonces(),
                  "interval_ms": PRODUCER_CONSTANTS["retained_heartbeat_ms"],
                  "timeout_ms": PRODUCER_CONSTANTS["connect_timeout_ms"]},
    )
    if launched is None:
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    s_name, s_uid = launched
    prot = layout.protected_namespace
    deadline = run.clock() + PRODUCER_CONSTANTS["pod_phase_timeout_ms"] / 1000
    if not await _await_log(run, prot, s_name, s_uid, "established", deadline):
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    endpoint = _established_endpoint(await run.cluster.pod_log(prot, s_name))
    sender_pod = await run.cluster.read_pod(layout.protected_namespace, s_name)
    if endpoint is None or sender_pod is None:
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    laddr, lport = endpoint
    established = await _allowed_control(run, broker, hold, node, family)
    if not await _terminate(run, _Pod(layout.protected_namespace, s_name,
                                      sender_pod["metadata"]["uid"], node, (laddr,))):
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    # Provoke reuse of the terminated sender's address (case 4 mechanism), then the
    # reusing pod resumes the ORIGINAL four-tuple from the spoofing template.
    reuser = await _provoke_reuse(run, laddr, node, hold, broker, family)
    if reuser is None:
        evidence["reused_address"] = laddr
        return _entry(run, key, verdict=("unavailable", "reuse_not_provoked"), evidence=evidence)
    start_ms = int(run.clock() * 1000)
    data_nonce, syn_nonce = _nonce(), _nonce()
    run.nonces.update([data_nonce, syn_nonce])
    try:
        await _start_sender(
            run, template="spoofing", namespace=layout.probe_namespace, node=node, labels={},
            argument={"packets": [
                # A data segment in the original four-tuple (last sequence state),
                {"src": laddr, "addr": hold_ip, "port": broker.port, "proto": "tcp",
                 "nonce": data_nonce, "sport": lport, "flags": 0x10, "with_payload": True},
                # plus a fresh SYN from the original source port.
                {"src": laddr, "addr": hold_ip, "port": broker.port, "proto": "tcp",
                 "nonce": syn_nonce, "sport": lport, "flags": 0x02},
            ], "repeat": PRODUCER_CONSTANTS["denial_tcp_attempts"],
               "interval_ms": PRODUCER_CONSTANTS["attempt_interval_ms"]},
        )
    except ProbeKitRefused:
        return _entry(run, key, verdict=("unavailable", "transport_unavailable"), evidence=evidence)
    end_ms = int(run.clock() * 1000) + PRODUCER_CONSTANTS["retained_bound_ms"] + PRODUCER_CONSTANTS[
        "listener_window_ms"
    ]
    deliveries, window = await _listener_deliveries(
        run, hold, nonces=[data_nonce, syn_nonce], peers=[laddr], start_ms=start_ms, end_ms=end_ms
    )
    control_ok = await _allowed_control(run, broker, hold, node, family)
    verdict = judge_retained(
        RetainedEvidence(
            established=established,
            listener_ready=window.ready,
            on_requested_node=reuser[0].node == node,
            family_address=True,
            post_trigger_deliveries=deliveries,
            sockets_sampled=window.sampled,
            window_covered=window.covered,
            overflow_moved=window.moved,
            trigger_confirmed=True,
            control_succeeded=control_ok,
            reuse_provoked=True,
        )
    )
    evidence.update({"sender_address": laddr, "source_port": lport, "reuse_provoked": True,
                     "deliveries": deliveries, "control_succeeded": control_ok})
    return _entry(run, key, verdict=verdict, evidence=evidence)


async def _provoke_reuse(
    run: _Run, address: str, node: str, twin: _Pod, broker: Target, family: str
) -> tuple[_Pod, list[str]] | None:
    """Create unselected pods on ``node`` until one takes ``address``, sending to the
    twin from its first instant. Returns the reusing pod and its nonces, or None."""
    twin_address = twin.address(family)
    if twin_address is None:
        return None
    deadline = run.clock() + PRODUCER_CONSTANTS["address_reuse_max_ms"] / 1000
    for _ in range(PRODUCER_CONSTANTS["address_reuse_max_pods"]):
        if run.clock() > deadline:
            return None
        attempts, nonces = _attempts([(twin_address, broker.port, p) for p in broker.protocols])
        candidate = await _start_sender(
            run, template="protected_probe", namespace=run.layout.protected_namespace,
            node=node, labels={}, argument=_sender_argument(attempts) | {"nonce_only": True},
        )
        if candidate is not None and candidate.address(family) == address:
            run.nonces.update(nonces)
            return candidate, nonces
    return None


# --- Case 7: node-local traffic and node-capability proof ---


async def case_seven(
    run: _Run, identities: Sequence[Subject] | None
) -> list[ObservationEntry]:
    entries: list[ObservationEntry] = []
    reaches = False
    addresses = await node_addresses(run)
    for family in run.layout.families:
        for placement in PLACEMENTS:
            entry = await _node_origin(run, family=family, placement=placement, addresses=addresses)
            entries.append(entry)
            if entry.outcome == "pass" and entry.reason == "reaches":
                reaches = True
    entries.append(await _rbac_node_capability(run, identities))
    entries.append(await _admission_node_capability(run))
    entries.append(await _running_node_capability(run, addresses))
    evidence = {"node_local_reaches": reaches}
    # If node-local traffic reaches and any capability entry is not pass, the
    # campaign must move to a separate authority domain (clause 4, case 7).
    if reaches and any(
        item.case == 7 and item.sub_case.endswith("_capability") and item.outcome != "pass"
        for item in entries
    ):
        run.node_authority_refusal = "separate_authority_domain_required"
    _ = evidence
    return entries


async def _node_origin(
    run: _Run, *, family: str, placement: str, addresses: Mapping[str, tuple[str, ...]] | None
) -> ObservationEntry:
    layout = run.layout
    key: EntryKey = (7, "node_origin", family, placement, "node")
    evidence: dict[str, Any] = {}
    twin_node = layout.eligible_nodes[0]
    probe_node = twin_node if placement == "same_node" else _other_node(layout, twin_node)
    if probe_node is None:
        return _entry(run, key, verdict=("unavailable", "single_eligible_node"), evidence=evidence)
    if addresses is None or family != "ipv4":
        # The kubelet readiness target is an address literal; the IPv6 form needs a
        # bracketed host the probe template does not take, so IPv6 is unavailable.
        return _entry(run, key, verdict=("unavailable", "node_probe_unavailable"),
                      evidence=evidence)
    port = layout.target("broker").port
    twin = await _start_listener(
        run,
        template="twin",
        namespace=layout.protected_namespace,
        node=twin_node,
        labels=layout.target("broker").role_labels,
        argument={"tcp": [port], "udp": [port]},
    )
    twin_ip = None if twin is None else twin.address(family)
    if twin is None or twin_ip is None:
        return _entry(run, key, verdict=("unavailable", "node_probe_unavailable"),
                      evidence=evidence)
    node_ips = {_canonical(a) for a in addresses.get(probe_node, ()) if address_family(a) == family}
    if not node_ips:
        return _entry(run, key, verdict=("unavailable", "node_probe_unavailable"),
                      evidence=evidence)
    # Let the twin sample at least once before the probe window opens, so the window
    # has a tick on each side of the kubelet connection.
    await run.sleep(1.0)
    start_ms = int(run.clock() * 1000)
    try:
        kubelet = await _start_listener(
            run,
            template="kubelet_probe",
            namespace=layout.probe_namespace,
            node=probe_node,
            labels={},
            argument={"probe_host": twin_ip, "probe_port": port},
        )
    except ProbeKitRefused:
        return _entry(run, key, verdict=("unavailable", "node_probe_unavailable"),
                      evidence=evidence)
    if kubelet is None:
        # The kubelet-probe pod never started, so no node-local traffic was driven.
        return _entry(run, key, verdict=("unavailable", "node_probe_unavailable"),
                      evidence=evidence)
    end_ms = int(run.clock() * 1000) + PRODUCER_CONSTANTS["listener_window_ms"]
    record = await _listener_past(run, twin, end_ms)
    sampled, covered, _moved = record.window(start_ms, end_ms)
    evidence.update({"node": probe_node, "node_addresses": sorted(node_ips),
                     "sampled": sampled, "covered": covered, "ticks": len(record.ticks)})
    if not (sampled and covered):
        # An incomplete listener window cannot prove the traffic was or was not seen.
        return _entry(run, key, verdict=("unavailable", "node_probe_unavailable"),
                      evidence=evidence)
    in_window = [
        item for item in record.events if start_ms <= item[4] <= end_ms and item[0] != "tick"
    ]
    from_node = [item for item in in_window if item[1] in node_ips]
    from_other = [item for item in in_window if item[1] not in node_ips]
    evidence.update({"twin": _pod_evidence(twin), "node": probe_node,
                     "node_addresses": sorted(node_ips), "from_node": len(from_node),
                     "from_other": len(from_other)})
    if from_other:
        # Traffic arrived from a non-node (e.g. tunnel/overlay) address, so it cannot
        # be attributed to a node origin: unavailable, never recorded as blocked.
        return _entry(run, key, verdict=("unavailable", "node_probe_unavailable"),
                      evidence=evidence)
    return _entry(run, key, verdict=judge_node_origin(observed=True, reached=bool(from_node)),
                  evidence=evidence)


async def _rbac_node_capability(
    run: _Run, identities: Sequence[Subject] | None
) -> ObservationEntry:
    key: EntryKey = (7, "rbac_node_capability", *NONE)
    if identities is None:
        return _entry(run, key, verdict=("unavailable", "review_unavailable"), evidence={})
    create_pods = [ResourceAttributes("create", "", "pods")]
    create_pods += [ResourceAttributes("create", g, r) for g, r in POD_SYNTHESIZING]
    create_pods.append(ResourceAttributes("create", "", "pods", subresource="ephemeralcontainers"))
    proxy = [
        ResourceAttributes(verb, "", "nodes", subresource="proxy") for verb in ("get", "create")
    ]
    # pass is decided by nodes/proxy only; the pod-creation rights are recorded so
    # admission_node_capability can show they are fenced (clause 4, case 7).
    _creatable, create_record = await _reviews(run, identities, create_pods)
    verdict, proxy_record = await _reviews(run, identities, proxy)
    return _entry(
        run, key, verdict=verdict,
        evidence={"nodes_proxy": proxy_record, "pod_creation": create_record},
    )


def _base_node_pod(run: _Run) -> dict[str, Any]:
    """A guard-admitted probe pod targeting an eligible node with no node-capable
    field: the non-vacuity control that admission must accept."""
    pod = probe_pod(
        template="twin",
        name=f"cni-{run.layout.campaign}-nodecap"[:63].rstrip("-"),
        namespace=run.layout.probe_namespace,
        node=run.layout.eligible_nodes[0],
        campaign=run.layout.campaign,
        role_labels={},
        argument={"tcp": [], "udp": []},
    )
    return pod


def _with_node_capability(pod: dict[str, Any], field: str, value: Any) -> dict[str, Any]:
    import copy

    pod = copy.deepcopy(pod)
    spec = pod["spec"]
    if field == "hostPath":
        spec["volumes"] = value["volumes"]
        spec["containers"][0]["volumeMounts"] = [{"name": "h", "mountPath": "/host"}]
    elif field == "privileged":
        spec["containers"][0]["securityContext"]["privileged"] = True
    elif field == "toleration":
        spec["tolerations"] = value
    else:
        spec[field] = value
    return pod


async def _admission_node_capability(run: _Run) -> ObservationEntry:
    """For each node-capable field, admission must admit an otherwise-identical
    templated base pod (non-vacuity) and refuse the pod with that field added.
    A guard that refuses the benign base makes the entry unavailable, not pass."""
    key: EntryKey = (7, "admission_node_capability", *NONE)
    cases: list[tuple[str, Any]] = list(NODE_CAPABLE)
    cases.append(
        ("toleration", [{"key": "curietech.ai/eligible-node", "operator": "Exists",
                         "effect": "NoSchedule"}])
    )
    base = _base_node_pod(run)
    verdicts: list[Verdict] = []
    record = []
    for field, value in cases:
        try:
            base_status, _ = await run.cluster.dry_run_create_pod(run.layout.probe_namespace, base)
            status, message = await run.cluster.dry_run_create_pod(
                run.layout.probe_namespace, _with_node_capability(base, field, value)
            )
        except ProbeKitRefused:
            base_status, status, message = None, None, ""
        if base_status is None or not (200 <= base_status < 300):
            # The benign base was not admitted, so a refusal of the offending pod
            # proves nothing about the node-capable field.
            verdict: Verdict = ("unavailable", "review_unavailable")
        else:
            verdict = judge_guard_dry_run(status, message, run.layout.guard_names)
        verdicts.append(verdict)
        record.append({"field": field, "base_status": base_status, "status": status,
                       "message": message[:512]})
    verdict = _combined(verdicts, ("pass", "guard_refused"))
    return _entry(run, key, verdict=verdict, evidence={"dry_runs": record})


async def _running_node_capability(
    run: _Run, addresses: Mapping[str, tuple[str, ...]] | None
) -> ObservationEntry:
    key: EntryKey = (7, "running_node_capability", *NONE)
    pods = await run.cluster.list_items("pods")
    if pods is None:
        return _entry(run, key, verdict=("unavailable", "review_unavailable"), evidence={})
    eligible = set(run.layout.eligible_nodes)
    allowed_namespaces = {
        run.layout.protected_namespace,
        run.layout.probe_namespace,
        run.layout.foreign_namespace,
        run.layout.release_namespace,
    }
    violations = []
    for pod in pods:
        spec = pod.get("spec", {})
        if spec.get("nodeName") not in eligible:
            continue
        meta = pod["metadata"]
        if meta.get("namespace") in allowed_namespaces:
            continue
        if _pod_is_system(run, meta, spec):
            continue
        if _pod_is_node_capable(spec):
            violations.append(f"{meta.get('namespace', '')}/{meta.get('name', '')}")
    return _entry(
        run, key, verdict=judge_running_nodes(len(violations)),
        evidence={"violations": sorted(violations)},
    )


def _pod_is_system(run: _Run, meta: Mapping[str, Any], spec: Mapping[str, Any]) -> bool:
    """A pod whose ServiceAccount is in the recorded system set is administrative or
    provider-owned (clause 6), so it is not an ordinary identity."""
    namespace = meta.get("namespace", "")
    account = spec.get("serviceAccountName") or spec.get("serviceAccount") or "default"
    if not namespace:
        return False
    try:
        subject = Subject("ServiceAccount", account, namespace)
    except ProbeKitRefused:
        return False
    return subject in run.layout.system_set


def _pod_is_node_capable(spec: Mapping[str, Any]) -> bool:
    if spec.get("hostNetwork") or spec.get("hostPID") or spec.get("hostIPC"):
        return True
    if any("hostPath" in volume for volume in spec.get("volumes", []) or []):
        return True
    # A privileged init or ephemeral container is node-capable too, not just the
    # main containers.
    for group in ("containers", "initContainers", "ephemeralContainers"):
        for container in spec.get(group, []) or []:
            if container.get("securityContext", {}).get("privileged"):
                return True
    return False


def _combined(verdicts: Sequence[Verdict], clean: Verdict) -> Verdict:
    for outcome in ("fail", "unavailable"):
        found = next((item for item in verdicts if item[0] == outcome), None)
        if found is not None:
            return found
    return clean if verdicts else ("unavailable", "review_unavailable")


def required_timing_entries(layout: ProbeLayout) -> set[EntryKey]:
    """Cases 3, 4, 5 and 7 entries that must all be ``pass`` (clause 4)."""
    keys: set[EntryKey] = set()
    for family in layout.families:
        for placement in PLACEMENTS:
            for sub_case in ("spoof_live", "spoof_terminated"):
                keys.add((3, sub_case, family, placement, "raw_socket"))
            for source in ("protected_unselected", "foreign_namespace"):
                keys.add((4, "new_pod", family, placement, source))
            for sub_case in ("policy_changed", "workload_replaced"):
                keys.add((5, sub_case, family, placement, "retained_peer"))
            keys.add((7, "node_origin", family, placement, "node"))
        keys.add((4, "address_reuse", family, "same_node", "protected_unselected"))
    for sub_case in (
        "rbac_node_capability",
        "admission_node_capability",
        "running_node_capability",
    ):
        keys.add((7, sub_case, *NONE))
    return keys
