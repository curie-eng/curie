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
    TEMPLATE_LABEL,
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
    if sub_case == "spoof_terminated":
        if not await _terminate(run, allowed):
            return ("unavailable", "phase_timeout"), record
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

    verdict = judge_denial(
        DenialEvidence(
            sender_ready=True,
            listener_ready=window.ready,
            on_requested_node=reuser.node == node,
            family_address=True,
            listener_deliveries=deliveries,
            sockets_sampled=window.sampled,
            window_covered=window.covered,
            overflow_moved=window.moved,
            client_handshakes=0,
            client_replies=0,
            control_succeeded=control_ok,
        ),
        delivery_reason="admitted_before_propagation",
    )
    evidence.update({"reused_address": target_ip, "reuser": _pod_evidence(reuser),
                     "deliveries": deliveries, "control_succeeded": control_ok})
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
    change_ms: int | None = None
    sender_log = ""
    listener_log = ""
    try:
        await _create_policy(run, allow, _probe_policy(allow, layout.probe_namespace,
                                                       "ret-listener", "ret-sender", port))
        listener = await _start_listener(
            run, template="twin", namespace=layout.probe_namespace, node=node,
            labels={_ROLE: "ret-listener"}, argument={"tcp": [port], "udp": [port], "hold": True},
        )
        listener_ip = None if listener is None else listener.address(family)
        if listener is None or listener_ip is None:
            return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
        nonces = _heartbeat_nonces()
        run.nonces.update(nonces)
        launched = await _launch(
            run, template="retained_sender", namespace=layout.probe_namespace, node=sender_node,
            labels={_ROLE: "ret-sender"},
            argument={"addr": listener_ip, "port": port, "nonces": nonces,
                      "interval_ms": PRODUCER_CONSTANTS["retained_heartbeat_ms"],
                      "timeout_ms": PRODUCER_CONSTANTS["connect_timeout_ms"]},
        )
        if launched is None:
            return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
        s_name, s_uid = launched
        deadline = run.clock() + PRODUCER_CONSTANTS["pod_phase_timeout_ms"] / 1000
        established = await _await_log(
            run, layout.probe_namespace, s_name, s_uid, "established", deadline
        )
        if not established:
            return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
        # A few heartbeats flow while still allowed, then remove the allowance.
        await run.sleep(_PRE_CHANGE_HEARTBEATS * PRODUCER_CONSTANTS["retained_heartbeat_ms"] / 1000)
        change_ms = int(run.clock() * 1000)
        await _delete_policy(run, allow)
        await _create_policy(run, deny, _probe_policy(deny, layout.probe_namespace,
                                                      "ret-listener", None, port))
        term_deadline = run.clock() + PRODUCER_CONSTANTS["pod_phase_timeout_ms"] / 1000
        await _await_terminal(run, layout.probe_namespace, s_name, s_uid, term_deadline)
        sender_log = await run.cluster.pod_log(layout.probe_namespace, s_name)
        await run.sleep(PRODUCER_CONSTANTS["listener_window_ms"] / 1000)
        listener_log = await run.cluster.pod_log(listener.namespace, listener.name)
    except ProbeKitRefused:
        return _entry(run, key, verdict=("unavailable", "transport_unavailable"), evidence=evidence)
    finally:
        await _delete_policy(run, allow)
        await _delete_policy(run, deny)
    return _retained_from_logs(run, key, sender_log, listener_log, change_ms, evidence)


def _heartbeat_nonces() -> list[str]:
    interval = PRODUCER_CONSTANTS["retained_heartbeat_ms"]
    span = PRODUCER_CONSTANTS["retained_bound_ms"] + 2 * PRODUCER_CONSTANTS["listener_window_ms"]
    return [_nonce() for _ in range(_PRE_CHANGE_HEARTBEATS + span // interval + 1)]


def _retained_from_logs(
    run: _Run, key: EntryKey, sender_log: str, listener_log: str, change_ms: int | None,
    evidence: dict[str, Any],
) -> ObservationEntry:
    try:
        sender = parse_retained_sender_log(sender_log)
        listener = parse_listener_log(listener_log)
    except ProbeKitRefused:
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    bound = PRODUCER_CONSTANTS["retained_bound_ms"]
    received = {nonce for _kind, _peer, _port, nonce, _at in listener.events}
    pre = [n for (n, at, result) in sender.sent if change_ms is not None and at <= change_ms
           and result == "ok"]
    post = [n for (n, at, result) in sender.sent if change_ms is not None
            and at > change_ms + bound]
    post_delivered = sum(1 for n in post if n in received)
    control_ok = any(n in received for n in pre)
    verdict = judge_retained(
        RetainedEvidence(
            established=sender.connected and change_ms is not None,
            listener_ready=listener.ready,
            on_requested_node=True,
            family_address=sender.local_address is not None,
            post_trigger_deliveries=post_delivered,
            sockets_sampled=True,
            window_covered=True,
            overflow_moved=False,
            trigger_confirmed=change_ms is not None,
            control_succeeded=control_ok,
            reuse_provoked=True,
        )
    )
    evidence.update({"change_ms": change_ms, "pre_delivered": control_ok,
                     "post_sent": len(post), "post_delivered": post_delivered})
    return _entry(run, key, verdict=verdict, evidence=evidence)


async def _workload_replaced(
    run: _Run, *, twin: _Pod, broker: Target, family: str, placement: str
) -> ObservationEntry:
    """Establish an allowed flow, replace the sender, then resume it after reuse.

    When the terminated address is not reused within the bound this is
    ``unavailable`` (``reuse_not_provoked``), the explicit ADR 0238 outcome; it is
    never a pass.
    """
    layout = run.layout
    key: EntryKey = (5, "workload_replaced", family, placement, "retained_peer")
    node = twin.node if placement == "same_node" else _other_node(layout, twin.node)
    evidence: dict[str, Any] = {"listener": _pod_evidence(twin)}
    if node is None:
        return _entry(run, key, verdict=("unavailable", "single_eligible_node"), evidence=evidence)
    hold = await _start_listener(
        run, template="twin", namespace=layout.protected_namespace, node=twin.node,
        labels=broker.role_labels, argument={"tcp": [broker.port], "udp": [broker.port],
                                             "hold": True},
    )
    hold_ip = None if hold is None else hold.address(family)
    if hold is None or hold_ip is None:
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    # Establish an allowed flow from an allowed-sender twin, then replace it.
    sender = await _start_listener(
        run, template="twin", namespace=layout.protected_namespace, node=node,
        labels=broker.allowed_sender_labels, argument={"tcp": [broker.port], "udp": [broker.port]},
    )
    sender_ip = None if sender is None else sender.address(family)
    if sender is None or sender_ip is None:
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    # The flow worked while allowed: an allowed sender reaches the hold twin.
    established = await _allowed_control(run, broker, hold, node, family)
    if not await _terminate(run, sender):
        return _entry(run, key, verdict=("unavailable", "phase_timeout"), evidence=evidence)
    # Provoke reuse of the terminated sender's address, then resume the flow from
    # the reusing pod. If connection tracking lets the old flow continue, the hold
    # twin receives data from the reused address.
    start_ms = int(run.clock() * 1000)
    reuser = await _provoke_reuse(run, sender_ip, node, hold, broker, family)
    if reuser is None:
        evidence["reused_address"] = sender_ip
        return _entry(run, key, verdict=("unavailable", "reuse_not_provoked"), evidence=evidence)
    end_ms = int(run.clock() * 1000) + PRODUCER_CONSTANTS["retained_bound_ms"] + PRODUCER_CONSTANTS[
        "listener_window_ms"
    ]
    deliveries, window = await _listener_deliveries(
        run, hold, nonces=reuser[1], peers=[sender_ip], start_ms=start_ms, end_ms=end_ms
    )
    control_ok = await _allowed_control(run, broker, hold, node, family)
    verdict = judge_retained(
        RetainedEvidence(
            established=established,
            listener_ready=window.ready,
            on_requested_node=True,
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
    evidence.update({"sender_address": sender_ip, "reuse_provoked": True,
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
    node_ips = [_canonical(a) for a in addresses.get(probe_node, ()) if address_family(a) == family]
    start_ms = int(run.clock() * 1000)
    try:
        await _start_listener(
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
    end_ms = int(run.clock() * 1000) + PRODUCER_CONSTANTS["listener_window_ms"]
    deliveries, _window = await _listener_deliveries(
        run, twin, nonces=[], peers=node_ips, start_ms=start_ms, end_ms=end_ms
    )
    evidence.update({"twin": _pod_evidence(twin), "node": probe_node, "node_addresses": node_ips,
                     "deliveries": deliveries})
    return _entry(run, key, verdict=judge_node_origin(observed=bool(node_ips),
                                                      reached=deliveries > 0), evidence=evidence)


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


def _node_capable_pod(run: _Run, field: str, value: Any) -> dict[str, Any]:
    pod = probe_pod(
        template="twin",
        name=f"cni-{run.layout.campaign}-nodecap"[:63].rstrip("-"),
        namespace=run.layout.probe_namespace,
        node=run.layout.eligible_nodes[0],
        campaign=run.layout.campaign,
        role_labels={},
        argument={"tcp": [], "udp": []},
    )
    del pod["metadata"]["labels"][TEMPLATE_LABEL]  # not a guard-admitted template
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
    key: EntryKey = (7, "admission_node_capability", *NONE)
    cases: list[tuple[str, Any]] = list(NODE_CAPABLE)
    cases.append(
        ("toleration", [{"key": "curietech.ai/eligible-node", "operator": "Exists",
                         "effect": "NoSchedule"}])
    )
    verdicts: list[Verdict] = []
    record = []
    for field, value in cases:
        try:
            status, message = await run.cluster.dry_run_create_pod(
                run.layout.probe_namespace, _node_capable_pod(run, field, value)
            )
        except ProbeKitRefused:
            status, message = None, ""
        verdicts.append(judge_guard_dry_run(status, message, run.layout.guard_names))
        record.append({"field": field, "status": status, "message": message[:512]})
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
    for container in spec.get("containers", []) or []:
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
