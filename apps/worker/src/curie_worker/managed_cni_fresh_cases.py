"""Actual case-five fresh-flow orchestration, Accepted ADR 0243.

@spec CNI-FRESH-FLOW-2 @spec CNI-FRESH-FLOW-3 @spec CNI-FRESH-FLOW-4.
Owned probe resources only; no protected policy writes or caller attestations.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, TypeVar

from .managed_cni_cases import (
    Target,
    _cleanup,
    _entry,
    _nonce,
    _other_node,
    _Pod,
    _pod_evidence,
    _resource_uid,
    _Run,
    _start,
)
from .managed_cni_fresh_programs import (
    FreshAttempt,
    FreshListenerRecord,
    FreshSenderRecord,
    parse_fresh_listener_log,
    parse_fresh_sender_log,
    parse_fresh_trigger_log,
)
from .managed_cni_probe_kit import (
    PLACEMENTS,
    PRODUCER_CONSTANTS,
    ObservationEntry,
    ProbeKitRefused,
    Verdict,
    address_family,
    sha256_canonical,
)

_ROLE = "curietech.ai/cni-fresh-role"
_T = TypeVar("_T")


class _Unavailable(ProbeKitRefused):
    def __init__(self, reason: str) -> None:
        super().__init__()
        self.reason = reason


def _need(value: bool, reason: str = "trigger_unconfirmed") -> None:
    if not value:
        raise _Unavailable(reason)


def _policy_binding(document: dict[str, Any]) -> dict[str, Any]:
    # A typed Kubernetes list item may omit TypeMeta. Validate it when present,
    # then retain every actual object field, including UID and resourceVersion.
    _need(document.get("apiVersion", "networking.k8s.io/v1") == "networking.k8s.io/v1")
    _need(document.get("kind", "NetworkPolicy") == "NetworkPolicy")
    return {key: value for key, value in document.items() if key not in {"apiVersion", "kind"}}


def _fresh_nonce(run: _Run) -> str:
    for _ in range(8):
        value = _nonce()
        if value not in run.nonces:
            run.nonces.add(value)
            return value
    raise _Unavailable("trigger_unconfirmed")


def _policy(
    name: str, namespace: str, selected: Mapping[str, str], allowed: Mapping[str, str], port: int
) -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "podSelector": {"matchLabels": dict(selected)},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [{"podSelector": {"matchLabels": dict(allowed)}}],
                    "ports": [{"protocol": "TCP", "port": port}],
                }
            ],
        },
    }


def _target(address: str, port: int, *, twin: bool) -> dict[str, Any]:
    return {"target": "twin" if twin else "real", "address": address, "port": port, "echo": twin}


@dataclass(slots=True)
class _Case:
    run: _Run
    family: str
    node: str
    broker: Target
    evidence: dict[str, Any] = field(default_factory=dict)
    deadline: float | None = None
    leaked: bool = False
    inventory: dict[str, dict[str, Any]] = field(default_factory=dict)
    listener_prefix: str = ""
    protected_snapshot: dict[str, dict[str, Any]] | None = None
    denial_sender_uid: str | None = None
    denial_challenge: str | None = None
    denial_targets: tuple[tuple[str, str, int], ...] = ()
    denial_attempts: tuple[FreshAttempt, ...] = ()
    sender_prefixes: dict[str, str] = field(default_factory=dict)

    async def call(self, operation: Awaitable[_T]) -> _T:
        # Close an unstarted coroutine when the aggregate budget is already gone.
        remaining = 120.0 if self.deadline is None else self.deadline - self.run.monotonic()
        if remaining <= 0:
            if hasattr(operation, "close"):
                operation.close()
            raise _Unavailable("phase_timeout")
        try:
            async with asyncio.timeout(remaining):
                value = await operation
        except TimeoutError:
            raise _Unavailable("phase_timeout") from None
        _need(self.deadline is None or self.run.monotonic() < self.deadline, "phase_timeout")
        return value

    async def pause(self, seconds: float) -> None:
        await self.call(self.run.sleep(seconds))

    async def policies(self) -> dict[str, dict[str, Any]]:
        items = await self.call(
            self.run.cluster.list_items("networkpolicies:" + self.run.layout.probe_namespace)
        )
        _need(items is not None)
        assert items is not None
        out = {}
        for item in items:
            uid = _resource_uid(item)
            _need(uid is not None and uid not in out)
            assert uid is not None
            _need(item.get("metadata", {}).get("namespace") == self.run.layout.probe_namespace)
            _need(isinstance(item.get("spec"), dict))
            out[uid] = _policy_binding(item)
        return out

    async def protected_shape(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Verify the role-only equivalence premise, or refuse unsupported recipes.

        Every protected ingress selector must use the one declared role key.
        Unknown labels/expressions or provider policies could select the real
        target differently from its twin, even when client replies are dropped.
        """
        role = dict(self.broker.role_labels)
        _need(len(role) == 1)
        role_key = next(iter(role))
        items = await self.call(
            self.run.cluster.list_items("networkpolicies:" + self.run.layout.protected_namespace)
        )
        _need(isinstance(items, list) and bool(items))
        assert items is not None
        current: dict[str, dict[str, Any]] = {}
        allowances, empty = [], []
        for raw in items:
            item = _policy_binding(raw)
            uid = _resource_uid(item)
            _need(uid is not None and uid not in current)
            assert uid is not None
            meta, spec = item.get("metadata", {}), item.get("spec", {})
            _need(
                meta.get("namespace") == self.run.layout.protected_namespace
                and isinstance(meta.get("resourceVersion"), str)
                and bool(meta["resourceVersion"])
            )
            current[uid] = item
            _need(isinstance(spec, dict))
            types = spec.get("policyTypes")
            _need(isinstance(types, list) and set(types) <= {"Ingress", "Egress"})
            selector = spec.get("podSelector")
            if selector in ({}, {"matchLabels": {}}):
                if "Ingress" not in types:
                    continue
                # Only deny-all is supported for a namespace-wide selector.
                _need(not spec.get("ingress"))
                empty.append(
                    {
                        "podSelector": copy.deepcopy(selector),
                        "policyTypes": ["Ingress"],
                    }
                )
                continue
            _need(isinstance(selector, dict) and set(selector) == {"matchLabels"})
            labels = selector["matchLabels"]
            _need(isinstance(labels, dict) and len(labels) == 1 and role_key in labels)
            if labels != role or "Ingress" not in types:
                continue
            # A copied ingress allowance must retain its complete supported
            # selector, peer, numeric port and protocol shapes. No synthesis
            # from an unverified Target description may replace these reads.
            _need(set(spec) <= {"podSelector", "policyTypes", "ingress"} and types == ["Ingress"])
            rules = spec.get("ingress", [])
            if not rules:
                empty.append(copy.deepcopy(spec))
                continue
            _need(isinstance(rules, list) and len(rules) == 1)
            rule = rules[0]
            _need(
                set(rule) == {"from", "ports"}
                and rule["from"]
                == [{"podSelector": {"matchLabels": dict(self.broker.allowed_sender_labels)}}]
            )
            ports = rule["ports"]
            _need(isinstance(ports, list) and bool(ports))
            observed_protocols = []
            for port in ports:
                _need(
                    isinstance(port, dict)
                    and set(port) == {"port", "protocol"}
                    and type(port["port"]) is int
                    and port["port"] == self.broker.port
                    and port["protocol"] in ("TCP", "UDP")
                )
                observed_protocols.append(port["protocol"].lower())
            _need(
                len(set(observed_protocols)) == len(observed_protocols)
                and set(observed_protocols) == set(self.broker.protocols)
            )
            allowances.append(copy.deepcopy(spec))
        for collection in self.run.layout.provider_policy_collections:
            # No provider-specific selector translation is implemented here.
            _need(await self.call(self.run.cluster.list_items(collection)) == [])
        _need(len(allowances) == 1)
        if self.protected_snapshot is None:
            self.protected_snapshot = current
            self.evidence["protected_policy_digest"] = sha256_canonical(current)
        else:
            _need(current == self.protected_snapshot)
        return allowances[0], empty

    async def check_policies(self) -> None:
        observed = await self.policies()
        _need(observed == self.inventory)
        await self.protected_shape()

    async def create_policy(
        self,
        name: str,
        selected: Mapping[str, str],
        allowed: Mapping[str, str],
        port: int,
        *,
        spec: dict[str, Any] | None = None,
    ) -> str:
        namespace = self.run.layout.probe_namespace
        document = _policy(name, namespace, selected, allowed, port)
        if spec is not None:
            document["spec"] = copy.deepcopy(spec)
        document["metadata"]["labels"] = {
            "curietech.ai/managed-cni-campaign": self.run.layout.campaign
        }
        self.run.pending_policies.add((namespace, name))
        created = await self.call(self.run.cluster.create_network_policy(namespace, document))
        uid = _resource_uid(created)
        _need(uid is not None)
        meta = created.get("metadata", {})
        _need(meta.get("name") == name and meta.get("namespace") == namespace)
        assert uid is not None
        self.run.created_policies.append((namespace, name, uid))
        self.run.pending_policies.remove((namespace, name))
        _need(
            created.get("spec") == document["spec"]
            and meta.get("labels") == document["metadata"]["labels"]
        )
        self.inventory[uid] = _policy_binding(created)
        await self.check_policies()
        return uid

    async def remove_policy(self, uid: str) -> None:
        document = self.inventory[uid]
        meta = document["metadata"]
        await self.call(
            self.run.cluster.delete_network_policy(meta["namespace"], meta["name"], uid)
        )
        wanted = {key: value for key, value in self.inventory.items() if key != uid}
        while True:
            observed = await self.policies()
            if observed == wanted:
                self.inventory = wanted
                return
            _need(observed == self.inventory)
            await self.pause(0.5)

    async def start(
        self,
        template: str,
        *,
        namespace: str,
        labels: Mapping[str, str],
        argument: Mapping[str, Any],
        node: str | None = None,
        finished: bool = False,
    ) -> _Pod:
        pod = await self.call(
            _start(
                self.run,
                template=template,
                namespace=namespace,
                node=node or self.node,
                labels=labels,
                argument=argument,
                finished=finished,
                deadline=self.deadline,
            )
        )
        _need(pod is not None, "phase_timeout")
        assert pod is not None
        _need(pod.address(self.family) is not None, "no_family_address")
        return pod

    async def same(self, pod: _Pod) -> dict[str, Any]:
        actual = await self.call(self.run.cluster.read_pod(pod.namespace, pod.name))
        _need(actual is not None and _resource_uid(actual) == pod.uid)
        assert actual is not None
        spec, meta, status = (
            actual.get("spec", {}),
            actual.get("metadata", {}),
            actual.get("status", {}),
        )
        _need(spec.get("nodeName") == pod.node)
        _need(tuple(item["ip"] for item in status.get("podIPs", [])) == pod.addresses)
        _need(sha256_canonical({"spec": spec, "labels": meta.get("labels")}) == pod.binding_digest)
        identities = status.get("containerStatuses", [])
        _need(len(identities) == 1)
        identity = identities[0]
        _need(
            type(identity.get("restartCount")) is int
            and identity["restartCount"] == 0
            and identity.get("containerID") == pod.container_id
            and identity.get("imageID") == pod.image_id
        )
        return actual

    def observe_denial(self, pod: _Pod, record: FreshSenderRecord) -> None:
        if pod.uid != self.denial_sender_uid:
            return
        _need(
            record.mode == "deny"
            and record.source == pod.address(self.family)
            and record.challenge in (None, self.denial_challenge)
        )
        wanted = [endpoint for endpoint in self.denial_targets for _ in range(5)]
        observed = [(a.target, a.destination, a.port) for a in record.attempts]
        _need(len(observed) <= len(wanted) and observed == wanted[: len(observed)])
        if record.attempts:
            _need(record.challenge == self.denial_challenge)
            self.denial_attempts = record.attempts
            self.leaked |= any(a.outcome == "connected" or a.echo for a in record.attempts)
            if self.leaked:
                self.evidence["attempts"] = [asdict(a) for a in record.attempts]

    async def sender(self, pod: _Pod, *, partial: bool = True) -> FreshSenderRecord:
        await self.same(pod)
        log = await self.call(self.run.cluster.pod_log(pod.namespace, pod.name))
        await self.same(pod)
        previous = self.sender_prefixes.get(pod.uid, "")
        _need(not previous or log.rstrip("\n") == previous or log.startswith(previous + "\n"))
        try:
            record = parse_fresh_sender_log(log, allow_partial=partial)
        except ProbeKitRefused:
            # A valid original, challenge-bound handshake stays a failure even
            # when a later row is malformed. No prefix can authorize a pass.
            if pod.uid == self.denial_sender_uid:
                for end in range(1, min(len(log.splitlines()), 128) + 1):
                    try:
                        prefix = parse_fresh_sender_log(
                            "\n".join(log.splitlines()[:end]), allow_partial=True
                        )
                        self.observe_denial(pod, prefix)
                    except ProbeKitRefused:
                        break
            raise
        _need(record.source == pod.address(self.family))
        self.observe_denial(pod, record)
        self.sender_prefixes[pod.uid] = log.rstrip("\n")
        return record

    async def listener(self, pod: _Pod) -> FreshListenerRecord:
        await self.same(pod)
        log = await self.call(self.run.cluster.pod_log(pod.namespace, pod.name))
        await self.same(pod)
        _need(
            not self.listener_prefix
            or log == self.listener_prefix
            or log.startswith(self.listener_prefix + "\n")
        )
        try:
            record = parse_fresh_listener_log(log)
        except ProbeKitRefused:
            # Valid prefixes are monotone until the first invalid row. Salvage
            # positive delivery only, in bounded logarithmic parser calls.
            rows = log.splitlines()
            low, high, latest = 1, len(rows), None
            while low <= high:
                middle = (low + high) // 2
                try:
                    candidate = parse_fresh_listener_log("\n".join(rows[:middle]))
                except ProbeKitRefused:
                    high = middle - 1
                else:
                    latest, low = candidate, middle + 1
            if latest is not None and latest.deliveries(self.denial_attempts):
                self.leaked = True
                self.evidence["listener_events"] = [list(event) for event in latest.events]
                self.evidence["listener_ticks"] = [list(tick) for tick in latest.ticks]
            raise
        self.listener_prefix = log.rstrip("\n")
        return record

    async def setup(self, sender: _Pod, listener: _Pod, nonce: str) -> FreshAttempt:
        record = await self.sender(sender)
        _need(record.setup is not None and record.challenge is None)
        assert record.setup is not None
        _need(
            record.setup.outcome == "connected"
            and record.setup.echo
            and record.setup.nonce == nonce + "ffff"
        )
        # The original allowed tuple must be corroborated at the owned receiver.
        received = await self.listener(listener)
        _need(received.deliveries((record.setup,)) > 0)
        self.evidence["setup"] = asdict(record.setup)
        return record.setup

    async def delete_pod(self, pod: _Pod) -> None:
        await self.call(self.run.cluster.delete_pod(pod.namespace, pod.name, pod.uid))
        while True:
            current = await self.call(self.run.cluster.read_pod(pod.namespace, pod.name))
            if current is None or (
                _resource_uid(current) is not None and _resource_uid(current) != pod.uid
            ):
                return
            await self.pause(0.5)

    def arguments(
        self,
        targets: list[dict[str, Any]],
        *,
        mode: str,
        excluded: list[int],
        setup_nonce: str | None = None,
        challenge: str | None = None,
    ) -> dict[str, Any]:
        value: dict[str, Any] = {
            "mode": mode,
            "family": self.family,
            "targets": targets,
            "excluded_ports": excluded,
            "setup": setup_nonce is not None,
        }
        if setup_nonce is not None:
            value["setup_nonce"] = setup_nonce
        if challenge is not None:
            value["challenge"] = challenge
        return value

    async def exercise(
        self,
        sender: _Pod,
        listener: _Pod,
        targets: list[dict[str, Any]],
        excluded: list[int],
        control_labels: Mapping[str, str],
        control_namespace: str,
    ) -> Verdict:
        # All trigger/identity observations precede independent nonce generation.
        await self.same(sender)
        initial = await self.listener(listener)
        _need(bool(initial.ticks) and initial.port == self.broker.port)
        await self.check_policies()
        self.deadline = self.run.monotonic() + 120
        challenge = _fresh_nonce(self.run)
        self.denial_sender_uid, self.denial_challenge = sender.uid, challenge
        self.denial_targets = tuple((t["target"], t["address"], t["port"]) for t in targets)
        self.evidence.update(
            trigger_nonce=challenge,
            sender=_pod_evidence(sender),
            listener=_pod_evidence(listener),
            targets=targets,
        )
        async with asyncio.timeout(120):
            trigger = await self.start(
                "fresh_trigger",
                namespace=self.run.layout.probe_namespace,
                labels={_ROLE: "trigger"},
                argument={
                    "family": self.family,
                    "address": sender.address(self.family),
                    "challenge": challenge,
                },
                finished=True,
            )
            await self.same(trigger)
            self.evidence["trigger_pod"] = _pod_evidence(trigger)
            trigger_record = parse_fresh_trigger_log(
                await self.call(self.run.cluster.pod_log(trigger.namespace, trigger.name))
            )
            _need(trigger_record.acknowledged and trigger_record.challenge == challenge)
            while True:
                record = await self.sender(sender)
                _need(record.challenge in (None, challenge))
                self.leaked |= any(
                    item.outcome == "connected" or item.echo for item in record.attempts
                )
                received = await self.listener(listener)
                self.leaked |= received.deliveries(record.attempts) > 0
                if record.complete:
                    break
                _need(record.refusal is None)
                await self.pause(0.5)
            expected = [
                (target["target"], target["address"], target["port"])
                for target in targets
                for _ in range(5)
            ]
            _need([(a.target, a.destination, a.port) for a in record.attempts] == expected)
            _need(not any(a.source_port in excluded for a in record.attempts))
            self.evidence["attempts"] = [asdict(a) for a in record.attempts]
            await self.pause(5)
            control_nonce = _fresh_nonce(self.run)
            used = sorted(set(excluded) | {a.source_port for a in record.attempts})
            control = await self.start(
                "fresh_allowed_control"
                if control_namespace == self.run.layout.protected_namespace
                else "fresh_control",
                namespace=control_namespace,
                labels=control_labels,
                argument=self.arguments(
                    targets, mode="control", excluded=used, challenge=control_nonce
                ),
                finished=True,
            )
            self.evidence["control_pod"] = _pod_evidence(control)
            controls = await self.sender(control, partial=False)
            control_ok = (
                controls.complete
                and controls.challenge == control_nonce
                and all(
                    a.outcome == "connected" and (a.target == "real" or a.echo)
                    for a in controls.attempts
                )
            )
            _need(len(controls.attempts) == len(targets), "control_failed")
            while True:
                received = await self.listener(listener)
                self.leaked |= received.deliveries(record.attempts) > 0
                covered = all(received.covers(a) for a in controls.attempts if a.target == "twin")
                if covered or not control_ok:
                    break
                await self.pause(0.5)
            await self.same(sender)
            await self.check_policies()
            self.evidence.update(
                control=[asdict(a) for a in controls.attempts],
                listener_events=[list(event) for event in received.events],
                listener_ticks=[list(tick) for tick in received.ticks],
            )
            if self.leaked:
                return "fail", "delivered"
            _need(control_ok, "control_failed")
            _need(covered, "listener_incomplete")
            _need(all(a.outcome == "timeout" for a in record.attempts), "unreachable_unattributed")
            return "pass", "denied"


async def _one(case: _Case, *, subcase: str, receiver_node: str) -> Verdict:
    run, layout, broker = case.run, case.run.layout, case.broker
    _need(broker.port != 42001 and "tcp" in broker.protocols, "destination_unknown")
    case.deadline = run.monotonic() + 120
    case.inventory = await case.policies()
    allowance_spec, default_specs = await case.protected_shape()
    suffix = _nonce()[:12]
    gate_labels = {_ROLE: "sender"}
    # A separate fixed ingress policy opens only the barrier, never a target port.
    await case.create_policy("gate-" + suffix, gate_labels, {_ROLE: "trigger"}, 42001)
    setup_nonce = _fresh_nonce(run)
    if subcase == "policy_changed":
        selected, allowed = dict(broker.role_labels), dict(broker.allowed_sender_labels)
        _need(_ROLE not in selected and _ROLE not in allowed)
        await case.create_policy("control-" + suffix, selected, {_ROLE: "control"}, broker.port)
        for index, spec in enumerate(default_specs):
            await case.create_policy(
                f"deny-{index}-" + suffix, selected, allowed, broker.port, spec=spec
            )
        allowance = await case.create_policy(
            "allow-" + suffix, selected, allowed, broker.port, spec=allowance_spec
        )
        listener = await case.start(
            "fresh_probe_listener",
            namespace=layout.probe_namespace,
            labels=selected,
            node=receiver_node,
            argument={"family": case.family, "port": broker.port},
        )
        address = listener.address(case.family)
        assert address is not None
        targets = [_target(address, broker.port, twin=True)]
        sender = await case.start(
            "fresh_sender",
            namespace=layout.probe_namespace,
            labels=allowed | gate_labels,
            argument=case.arguments(targets, mode="deny", excluded=[], setup_nonce=setup_nonce),
        )
        old = await case.setup(sender, listener, setup_nonce)
        await case.remove_policy(allowance)
        await case.same(sender)
        return await case.exercise(
            sender, listener, targets, [old.source_port], {_ROLE: "control"}, layout.probe_namespace
        )
    listener = await case.start(
        "fresh_twin",
        namespace=layout.protected_namespace,
        labels=broker.role_labels,
        node=receiver_node,
        argument={"family": case.family, "port": broker.port},
    )
    address = listener.address(case.family)
    assert address is not None
    actual_addresses = [a for a in broker.real_addresses if address_family(a) == case.family]
    _need(
        bool(actual_addresses)
        and len(set(actual_addresses)) == len(actual_addresses)
        and len(actual_addresses) <= 7,
        "destination_unknown",
    )
    targets = [
        _target(address, broker.port, twin=True),
        *[_target(a, broker.port, twin=False) for a in actual_addresses],
    ]
    original = await case.start(
        "fresh_setup",
        namespace=layout.protected_namespace,
        labels=broker.allowed_sender_labels,
        argument=case.arguments(targets, mode="setup", excluded=[], setup_nonce=setup_nonce),
    )
    old = await case.setup(original, listener, setup_nonce)
    await case.delete_pod(original)
    reuse_deadline = run.monotonic() + PRODUCER_CONSTANTS["address_reuse_max_ms"] / 1000
    case.deadline = reuse_deadline
    reused_sender = None
    for _ in range(PRODUCER_CONSTANTS["address_reuse_max_pods"]):
        candidate = await case.start(
            "fresh_sender",
            namespace=layout.probe_namespace,
            labels=gate_labels,
            argument=case.arguments(targets, mode="deny", excluded=[old.source_port]),
        )
        if candidate.address(case.family) == old.source:
            reused_sender = candidate
            break
        await case.delete_pod(candidate)
    _need(reused_sender is not None, "reuse_not_provoked")
    assert reused_sender is not None
    case.evidence["terminated_original"] = _pod_evidence(original)
    return await case.exercise(
        reused_sender,
        listener,
        targets,
        [old.source_port],
        broker.allowed_sender_labels,
        layout.protected_namespace,
    )


async def case_five_fresh(run: _Run, twins: Mapping[str, _Pod]) -> list[ObservationEntry]:
    """Keep the complete matrix, exact triggers, post-trigger controls and cleanup."""
    entries = []
    stop_reason: str | None = None
    broker = run.layout.target("broker")
    anchor = twins.get("broker")
    for family in run.layout.families:
        for placement in PLACEMENTS:
            receiver_node = None if anchor is None else anchor.node
            sender_node = (
                receiver_node
                if placement == "same_node"
                else _other_node(run.layout, receiver_node or "")
            )
            for subcase in ("policy_changed", "workload_replaced"):
                evidence: dict[str, Any] = {}
                verdict: Verdict = "unavailable", "phase_timeout"
                if stop_reason is not None:
                    verdict = "unavailable", stop_reason
                elif receiver_node is None or sender_node is None:
                    verdict = "unavailable", "single_eligible_node"
                else:
                    child = _Run(
                        run.cluster,
                        run.layout,
                        run.clock,
                        run.sleep,
                        counter=run.counter,
                        monotonic=run.monotonic,
                        node_locks=run.node_locks,
                    )
                    case = _Case(child, family, sender_node, broker, evidence)
                    try:
                        async with run.node_lock(sender_node):
                            verdict = await _one(case, subcase=subcase, receiver_node=receiver_node)
                    except _Unavailable as exc:
                        verdict = "unavailable", exc.reason
                    except TimeoutError:
                        verdict = "unavailable", "phase_timeout"
                    except ProbeKitRefused:
                        verdict = "unavailable", "transport_unavailable"
                    finally:
                        leftovers = await _cleanup(child)
                        evidence["leftovers"] = leftovers
                        run.counter = child.counter
                        run.created.extend(child.created)
                        run.created_policies.extend(child.created_policies)
                        run.pending_pods.update(child.pending_pods)
                        run.pending_policies.update(child.pending_policies)
                        run.nonces.update(child.nonces)
                    if leftovers:
                        stop_reason = "cleanup_unconfirmed"
                        run.cleanup_unconfirmed = True
                    if case.leaked:
                        verdict = "fail", "delivered"
                    elif leftovers:
                        verdict = "unavailable", "cleanup_unconfirmed"
                entries.append(
                    _entry(
                        run,
                        (5, subcase, family, placement, "retained_peer"),
                        verdict=verdict,
                        evidence=evidence,
                    )
                )
    return entries
