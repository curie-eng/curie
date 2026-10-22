"""Case flow of the managed CNI probe kit against a modelled cluster, ADR 0238.

@spec MANAGED-CNI-4. ``ModelCluster`` stands in for the API server and a pod
network whose enforcement the test chooses. It proves case order, verdict
wiring, refusal reasons and cleanup only. It observes no policy engine; the
behavioral evidence comes from a real multi-node cluster.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

import pytest
from curie_worker.managed_cni_cases import (
    Destination,
    ProbeKit,
    ProbeLayout,
    ResourceAttributes,
    Subject,
    Target,
    required_entries,
)
from curie_worker.managed_cni_probe_kit import Measurement, ProbeKitRefused, accepts

ROLE = "curietech.ai/policy-role"
PROBE_ROLE = "curietech.ai/managed-cni-probe-role"
BROKER = Target(
    name="broker",
    role_labels={ROLE: "broker"},
    allowed_sender_labels={ROLE: "broker-client"},
    port=16379,
    protocols=("tcp", "udp"),
    closed_port=16380,
    real_addresses=("10.96.0.10",),
)
MEDIATOR = Target(
    name="mediator",
    role_labels={ROLE: "mediator"},
    allowed_sender_labels={ROLE: "mediator-client"},
    port=18443,
    protocols=("tcp",),
    closed_port=18444,
    real_addresses=("10.96.0.11",),
)
PEER = Destination("10.0.9.9", 18080, "tcp")
GUARD = "curie-protected-guard.curietech.ai"
LAYOUT = ProbeLayout(
    campaign="c1",
    protected_namespace="prot",
    probe_namespace="probe-a",
    foreign_namespace="probe-foreign",
    release_namespace="release",
    eligible_nodes=("n1", "n2"),
    families=("ipv4",),
    targets=(BROKER, MEDIATOR),
    runner_labels={ROLE: "runner"},
    declared_peers=(PEER,),
    supplied_egress={
        "instance_metadata": (Destination("169.254.169.254", 80, "tcp"),),
        "kubernetes_api": (Destination("10.96.0.1", 443, "tcp"),),
        "cloud_network": (Destination("10.20.0.5", 443, "tcp"),),
    },
    guard_names=(GUARD,),
    system_set=frozenset({Subject("Group", "system:masters")}),
    excluded=frozenset({Subject("ServiceAccount", "measuring", "control")}),
    provider_policy_collections=(),
    policy_agent_objects=(),
)


class ModelCluster:
    """API server plus a pod network that enforces role-label policy when told to."""

    def __init__(self, *, enforcing: bool = True, control_reachable: bool = True) -> None:
        self.enforcing = enforcing
        self.control_reachable = control_reachable
        self.now = 1_000_000.0
        self.pods: dict[tuple[str, str], dict[str, Any]] = {}
        self.logs: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.ip_owner: dict[str, tuple[str, str]] = {}
        self.next_ip = 1
        self.dry_run: tuple[int | None, str] = (
            400,
            f'admission webhook "{GUARD}" denied the request: template',
        )
        self.allowed_reviews: set[tuple[str, str]] = set()
        self.policy_version = "1"
        self.bump_policy_after_reads: int | None = None
        self.policy_reads = 0
        self.created = 0
        self.dropped: set[str] = set()  # addresses the environment drops for every pod
        self.forgeries: list[dict[str, str]] = []
        self.admit_forgery_roles: set[str] = set()
        # A guard that fails to fence node-capable fields on templated pods (case 7).
        self.admit_node_capable = False
        self.real_attempts: list[tuple[str, str]] = []
        # Per node, the model time until which a sender's address and window are in use.
        self.busy_until: dict[str, float] = {}
        self.overlaps: list[str] = []
        self.extra_bindings: list[dict[str, Any]] = []
        # Freed pod addresses, handed back out when ``recycle`` is set, so address
        # reuse (cases 4 and 5) can be provoked deterministically.
        self.free_ips: list[str] = []
        self.recycle = True
        self.node_ips = {"n1": "192.168.49.2", "n2": "192.168.49.3"}
        # Retained connections (case 5), finalized once their window elapses.
        self.retained: dict[tuple[str, str], dict[str, Any]] = {}
        self.ret_deny_ms: int | None = None
        self.ret_deny_name: str | None = None
        self.policies: dict[str, dict[str, Any]] = {}
        # Source roles currently allowed to the probe-namespace "ret-listener"
        # (case 5), recomputed as probe policies are created and deleted.
        self.probe_allowed_roles: set[str] = set()
        # A CNI that lets a forged source through even while enforcing (case 3), or
        # a connection-tracking engine that keeps a flow after a policy change
        # (case 5), each a failing cluster.
        self.spoof_leaks = False
        self.retained_leaks = False
        # Case 7: the model's CNI admits node-local (kubelet) traffic to local pods.
        self.node_local_reaches = True
        # Node-local traffic arrives from a tunnel/overlay address, not a node IP.
        self.node_local_tunnel = False
        # The kubelet-probe pod's readiness probe never executes (no Ready condition).
        self.kubelet_readiness_absent = False
        # The post-change liveness control (ret-control) cannot reach the listener.
        self.drop_liveness = False
        # A sender writes no result line (case 4: a missing sender record).
        self.sender_result_missing = False
        # The raw-socket injector cannot send (case 5 workload_replaced).
        self.spoof_send_fails = False
        # The retained sender sends no heartbeat after the policy change (case 5).
        self.retained_no_post = False
        # Confirming address absence takes longer than the 30 s freshness bound.
        self.slow_absence = False
        # Templates created, as (namespace, template), to assert placement (case 5/8).
        self.created_templates: list[tuple[str, str]] = []
        # Any ordinary identity allowed node-capable rights, by (identity, resource).
        self.extra_pods: list[dict[str, Any]] = []
        # Seconds a created pod stays Pending (container creating). Until then the
        # API reports no address and refuses its log, as a real API server does.
        self.container_start_s = 0.0

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)  # let other probes interleave, as real waits do

    def _ms(self) -> int:
        return int(self.now * 1000)

    def _program(self, argument: dict[str, Any]) -> str:
        if "packets" in argument:
            return "spoof"
        if "nonces" in argument:
            return "retained_sender"
        if "phases" in argument:
            return "sender"
        if "probe_host" in argument:
            return "idle"
        return "listener"

    async def create_pod(self, namespace: str, document: dict[str, Any]) -> dict[str, Any]:
        name = document["metadata"]["name"]
        assert (namespace, name) not in self.pods
        self.now += 1.0  # pod start latency
        self.created += 1
        if self.recycle and self.free_ips:
            ip = self.free_ips.pop(0)
        else:
            ip = f"10.0.{self.next_ip // 250}.{self.next_ip % 250 + 1}"
            self.next_ip += 1
        uid = f"uid-{self.created}"
        argument = json.loads(document["spec"]["containers"][0]["command"][4])
        pod = {
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": uid,
                "labels": document["metadata"]["labels"],
            },
            "spec": {"nodeName": document["spec"]["nodeName"]},
            "status": {"phase": "Running", "podIPs": [{"ip": ip}]},
            "program": self._program(argument),
            "argument": argument,
            "starts_at": self.now + self.container_start_s,
        }
        self.pods[(namespace, name)] = pod
        self.ip_owner[ip] = (namespace, name)
        self.logs[(namespace, name)] = []
        template = pod["metadata"]["labels"].get("curietech.ai/managed-cni-probe-template", "")
        self.created_templates.append((namespace, template))
        kind = pod["program"]
        if kind == "listener":
            self.logs[(namespace, name)].append({"ev": "ready", "t": self._ms()})
        elif kind == "idle":
            self.logs[(namespace, name)].append({"ev": "ready", "t": self._ms()})
            # The readiness probe runs unless told otherwise, leaving a Ready condition.
            if not self.kubelet_readiness_absent:
                pod["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
            self._kubelet(pod)
        elif kind == "spoof":
            self._spoof_send(pod)
            pod["status"]["phase"] = "Succeeded"
        elif kind == "retained_sender":
            self._retained_create(pod)
        else:
            self._send(pod)
            pod["status"]["phase"] = "Succeeded"
        return {"metadata": {"uid": uid}}

    def _spoof_send(self, sender: dict[str, Any]) -> None:
        packets = []
        for item in sender["argument"]["packets"]:
            nonce = item["nonce"]
            owner = self.ip_owner.get(item["addr"])
            # The CNI drops a forged source address when enforcing; otherwise the
            # listener records it. A four-tuple injection (a flagged segment) also
            # gets through when connection tracking retains the flow (retained_leaks).
            injection = "flags" in item
            leak = not self.enforcing or self.spoof_leaks or (injection and self.retained_leaks)
            if owner is not None and leak:
                self.logs[owner].append(
                    {"ev": item["proto"] if item["proto"] in ("tcp", "udp") else "tcp",
                     "peer": item["src"], "port": item["port"], "nonce": nonce, "t": self._ms()}
                )
            if self.spoof_send_fails:
                packets.append([nonce, "error:Blocked"])
            else:
                packets.append([nonce, "sent" if ":" not in item["src"] else "unsupported_family"])
        self.now += 0.2
        self.logs[(sender["metadata"]["namespace"], sender["metadata"]["name"])].append(
            {"ev": "result", "packets": packets, "t": self._ms()}
        )

    def _kubelet(self, pod: dict[str, Any]) -> None:
        argument = pod["argument"]
        owner = self.ip_owner.get(argument["probe_host"])
        node_ip = self.node_ips.get(pod["spec"]["nodeName"])
        # A tunnel/overlay source address is not a node address: the producer cannot
        # attribute it to a node origin.
        peer = "10.244.7.7" if self.node_local_tunnel else node_ip
        if owner is not None and peer is not None and self.node_local_reaches:
            self.logs[owner].append(
                {"ev": "tcp", "peer": peer, "port": argument["probe_port"], "nonce": "",
                 "t": self._ms()}
            )

    def _retained_create(self, pod: dict[str, Any]) -> None:
        key = (pod["metadata"]["namespace"], pod["metadata"]["name"])
        ip = pod["status"]["podIPs"][0]["ip"]
        argument = pod["argument"]
        owner = self.ip_owner.get(argument["addr"])
        self.logs[key].append({"ev": "established", "laddr": ip, "lport": 54321, "t": self._ms()})
        self.retained[key] = {
            "established_ms": self._ms(),
            "nonces": argument["nonces"],
            "interval": argument["interval_ms"],
            "owner": owner,
            "ip": ip,
            "finalized": False,
        }

    def _finalize_retained(self, key: tuple[str, str]) -> None:
        state = self.retained[key]
        if state["finalized"]:
            return
        established, interval = state["established_ms"], state["interval"]
        sent = []
        for i, nonce in enumerate(state["nonces"]):
            at = established + i * interval
            if at > self._ms():
                break
            # Model a sender that stops once the allowance is removed: no post heartbeat.
            if self.retained_no_post and self.ret_deny_ms is not None and at > self.ret_deny_ms:
                break
            sent.append([nonce, at, "ok"])
            # Enforcing cuts the flow at the policy change; a conntrack engine that
            # retains the flow (retained_leaks), or a non-enforcing CNI, keeps it.
            delivered = self.enforcing and (self.ret_deny_ms is None or at < self.ret_deny_ms)
            if not self.enforcing or self.retained_leaks:
                delivered = True
            if delivered and state["owner"] is not None:
                self.logs[state["owner"]].append(
                    {"ev": "tcp", "peer": state["ip"], "port": 0, "nonce": nonce, "t": at}
                )
        self.logs[key].append(
            {"ev": "result", "connected": True, "laddr": state["ip"], "lport": 54321, "sent": sent}
        )
        self.pods[key]["status"]["phase"] = "Succeeded"
        state["finalized"] = True

    def _admits(self, sender: dict[str, Any], listener: dict[str, Any], port: int) -> bool:
        if not self.enforcing:
            return True
        labels = sender["metadata"]["labels"]
        # Case 5 probe-namespace listener: delivery follows the probe policies.
        if listener["metadata"]["labels"].get(PROBE_ROLE) == "ret-listener":
            if self.drop_liveness and labels.get(PROBE_ROLE) == "ret-control":
                return False
            return labels.get(PROBE_ROLE) in self.probe_allowed_roles
        for target in (BROKER, MEDIATOR):
            if listener["metadata"]["labels"].get(ROLE) == target.role_labels[ROLE]:
                allowed = labels.get(ROLE) == target.allowed_sender_labels[ROLE]
                return allowed and port == target.port and self.control_reachable
        return False

    def _send(self, sender: dict[str, Any]) -> None:
        node = sender["spec"]["nodeName"]
        if self.busy_until.get(node, 0.0) > self.now:
            self.overlaps.append(node)
        start = self._ms()
        results = []
        for phase in sender["argument"]["phases"]:
            for item in phase:
                address, port, nonce = item["addr"], item["port"], item["nonce"]
                real = {t.real_addresses[0]: t for t in (BROKER, MEDIATOR)}
                owner = self.ip_owner.get(address)
                if owner is not None:
                    listener = self.pods[owner]
                    delivered = self._admits(sender, listener, port)
                    if delivered:
                        self.logs[owner].append(
                            {
                                "ev": item["proto"],
                                "peer": sender["status"]["podIPs"][0]["ip"],
                                "port": port,
                                "nonce": nonce,
                                "t": self._ms(),
                            }
                        )
                elif address in real:
                    self.real_attempts.append((address, item["proto"]))
                    delivered = not self.enforcing
                elif address == PEER.address:
                    delivered = sender["metadata"]["labels"].get(ROLE) == "runner"
                else:
                    # The probe namespace carries no egress policy.
                    unrestricted = sender["metadata"]["namespace"] == "probe-a"
                    delivered = (unrestricted or not self.enforcing) and address not in self.dropped
                result = (
                    ("connected" if item["proto"] in ("tcp", "dns_tcp") else "reply")
                    if (delivered)
                    else "timeout"
                )
                results.append(dict(item, result=result, start=self._ms()))
                self.now += 0.2
        self.now += 2.0
        # A denial sender's address may be reused once it ends while its listener
        # window still runs; no other probe pod may start on that node meanwhile.
        # Nonce-attributed senders (first-packet) are exempt: they never attribute a
        # delivery by source address, so sharing a node cannot misattribute.
        template = sender["metadata"]["labels"]["curietech.ai/managed-cni-probe-template"]
        if template in ("probe_sender", "protected_probe", "release_probe") and not sender[
            "argument"
        ].get("nonce_only"):
            self.busy_until[node] = self.now + 5.0
        if self.sender_result_missing:
            # Model a sender whose result line never appears: no record to parse.
            return
        self.logs[(sender["metadata"]["namespace"], sender["metadata"]["name"])].append(
            {"ev": "result", "start": start, "end": self._ms(), "attempts": results}
        )

    async def read_pod(self, namespace: str, name: str) -> dict[str, Any] | None:
        key = (namespace, name)
        state = self.retained.get(key)
        if state is not None and not state["finalized"]:
            finish = state["established_ms"] + len(state["nonces"]) * state["interval"]
            if self._ms() >= finish:
                self._finalize_retained(key)
        pod = self.pods.get(key)
        if pod is not None and self.now < pod["starts_at"]:
            return {**pod, "status": {"phase": "Pending"}}
        return pod

    async def pod_log(self, namespace: str, name: str) -> str:
        pod = self.pods[(namespace, name)]
        if self.now < pod["starts_at"]:
            # HTTP 400: container "probe" is waiting to start: ContainerCreating.
            raise ProbeKitRefused
        lines = list(self.logs[(namespace, name)])
        if pod["program"] == "listener":
            first = lines[0]["t"]
            lines += [
                {"ev": "tick", "sockets": True, "overflow": 0, "t": t}
                for t in range(first, self._ms() + 1, 500)
            ]
        return "\n".join(json.dumps(line) for line in lines)

    async def delete_pod(self, namespace: str, name: str, uid: str) -> None:
        pod = self.pods.get((namespace, name))
        if pod is not None and pod["metadata"]["uid"] == uid:
            for item in pod["status"].get("podIPs", []):
                ip = item.get("ip")
                if ip is not None:
                    self.ip_owner.pop(ip, None)
                    self.free_ips.append(ip)
            del self.pods[(namespace, name)]
            self.retained.pop((namespace, name), None)

    def _recompute_probe_roles(self) -> None:
        allowed: set[str] = set()
        for document in self.policies.values():
            spec = document["spec"]
            role = spec.get("podSelector", {}).get("matchLabels", {}).get(PROBE_ROLE)
            if role != "ret-listener":
                continue
            for rule in spec.get("ingress", []):
                for peer in rule.get("from", []):
                    source = peer.get("podSelector", {}).get("matchLabels", {}).get(PROBE_ROLE)
                    if source:
                        allowed.add(source)
        self.probe_allowed_roles = allowed

    async def create_network_policy(
        self, namespace: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        name = document["metadata"]["name"]
        self.policies[name] = document
        spec = document["spec"]
        role = spec.get("podSelector", {}).get("matchLabels", {}).get(PROBE_ROLE)
        # An empty-ingress policy on the retained listener removes the allowance.
        if role == "ret-listener" and not spec.get("ingress"):
            self.ret_deny_ms, self.ret_deny_name = self._ms(), name
        self._recompute_probe_roles()
        return {"metadata": {"uid": f"np-{name}"}}

    async def delete_network_policy(self, namespace: str, name: str) -> None:
        self.policies.pop(name, None)
        if name == self.ret_deny_name:
            self.ret_deny_ms, self.ret_deny_name = None, None
        self._recompute_probe_roles()

    async def dry_run_create_pod(
        self, namespace: str, document: dict[str, Any]
    ) -> tuple[int | None, str]:
        labels = document["metadata"]["labels"]
        self.forgeries.append(labels)
        spec = document["spec"]
        if "curietech.ai/managed-cni-probe-template" in labels:
            # Case 7: the guard fences node-capable fields even on a templated pod,
            # but admits an otherwise-identical benign pod (non-vacuity).
            if self._node_capable_spec(spec) and not self.admit_node_capable:
                return self.dry_run
            return (201, "")
        # Case 2 label forgery: a pod carrying selector labels but no template label.
        if labels.get(ROLE) in self.admit_forgery_roles:
            return (201, "")
        return self.dry_run

    @staticmethod
    def _node_capable_spec(spec: dict[str, Any]) -> bool:
        if spec.get("hostNetwork") or spec.get("hostPID") or spec.get("hostIPC"):
            return True
        if any("hostPath" in v for v in spec.get("volumes", []) or []):
            return True
        for group in ("containers", "initContainers", "ephemeralContainers"):
            containers = spec.get(group, []) or []
            if any(c.get("securityContext", {}).get("privileged") for c in containers):
                return True
        return any(
            t.get("key") == "curietech.ai/eligible-node" for t in spec.get("tolerations", []) or []
        )

    async def access_review(
        self, user: str, groups: Sequence[str], attributes: ResourceAttributes
    ) -> bool | None:
        identity = groups[0] if user == "curie-probe:ordinary-group-member" else user
        return (identity, attributes.resource) in self.allowed_reviews

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        if collection == "nodes":
            return [
                {
                    "metadata": {"name": "n1"},
                    "status": {"addresses": [{"type": "InternalIP", "address": "192.168.49.2"}]},
                },
                {
                    "metadata": {"name": "n2"},
                    "status": {"addresses": [{"type": "InternalIP", "address": "192.168.49.3"}]},
                },
            ]
        if collection == "pods":
            if self.slow_absence:
                # Confirming absence takes longer than the 30 s freshness bound.
                self.now += 31.0
                self.slow_absence = False
            return [
                {
                    "metadata": {"name": pod["metadata"]["name"],
                                 "namespace": pod["metadata"]["namespace"]},
                    "spec": pod["spec"],
                    "status": pod["status"],
                }
                for pod in self.pods.values()
            ] + self.extra_pods
        if collection == "clusterrolebindings":
            return [
                {
                    "metadata": {"name": "admin"},
                    "subjects": [{"kind": "Group", "name": "system:masters"}],
                },
                {"metadata": {"name": "dev"}, "subjects": [{"kind": "User", "name": "alice"}]},
                *self.extra_bindings,
            ]
        if collection == "rolebindings":
            return [
                {
                    "metadata": {"name": "m", "namespace": "control"},
                    "subjects": [
                        {"kind": "ServiceAccount", "name": "measuring", "namespace": "control"}
                    ],
                },
            ]
        assert collection == "networkpolicies:prot"
        self.policy_reads += 1
        if self.bump_policy_after_reads == self.policy_reads:
            self.policy_version = "2"
        return [
            {
                "metadata": {
                    "name": "broker-ingress",
                    "namespace": "prot",
                    "uid": "np-1",
                    "resourceVersion": self.policy_version,
                }
            }
        ]


def measure(cluster: ModelCluster, layout: ProbeLayout = LAYOUT) -> Measurement:
    kit = ProbeKit(cluster, layout, clock=cluster.clock, sleep=cluster.sleep)
    return asyncio.run(kit.measure())


def outcomes(measurement: Measurement) -> dict[tuple[int, str, str, str, str], tuple[str, str]]:
    return {item.key: (item.outcome, item.reason) for item in measurement.entries}


def test_enforcing_two_node_cluster_passes_every_entry_it_can_measure() -> None:
    cluster = ModelCluster()
    measurement = measure(cluster)
    result = outcomes(measurement)
    # Every entry passes except case 5 workload_replaced, which is unavailable by
    # design: the original TCP sequence state cannot be reproduced from user space,
    # so the campaign refuses on it (an enforcing CNI never leaks, so no fail).
    assert measurement.refusal == "sequence_state_unavailable", result
    assert result[(5, "workload_replaced", "ipv4", "same_node", "retained_peer")] == (
        "unavailable",
        "sequence_state_unavailable",
    )
    others = {k: v for k, v in result.items() if k[:2] != (5, "workload_replaced")}
    assert all(outcome == "pass" for outcome, _ in others.values()), others
    assert not accepts(measurement.entries, required_entries(LAYOUT))
    assert cluster.pods == {}  # every probe pod was deleted by UID


def test_non_enforcing_cluster_refuses_at_case_one_and_runs_nothing_else() -> None:
    cluster = ModelCluster(enforcing=False)
    measurement = measure(cluster)
    assert measurement.refusal == "enforcement_absent"
    assert {item.case for item in measurement.entries} == {1}
    assert {item.outcome for item in measurement.entries} == {"fail"}
    assert {item.reason for item in measurement.entries} == {"enforcement_absent"}
    assert not accepts(measurement.entries, required_entries(LAYOUT))
    assert cluster.pods == {}


def test_single_eligible_node_cannot_measure_cross_node() -> None:
    layout = replace(LAYOUT, eligible_nodes=("n1",))
    measurement = measure(ModelCluster(), layout)
    assert measurement.refusal == "non_vacuity_unavailable"
    cross = [item for item in measurement.entries if item.placement == "cross_node"]
    assert cross and {(i.outcome, i.reason) for i in cross} == {
        ("unavailable", "single_eligible_node")
    }


def test_a_dead_listener_never_reads_as_enforcement() -> None:
    measurement = measure(ModelCluster(control_reachable=False))
    assert measurement.refusal == "non_vacuity_unavailable"
    assert {(i.outcome, i.reason) for i in measurement.entries} == {
        ("unavailable", "control_failed")
    }


def test_guard_admission_and_allowed_reviews_fail() -> None:
    cluster = ModelCluster()
    cluster.dry_run = (201, "")
    cluster.allowed_reviews = {
        ("alice", "deployments"),
        ("system:authenticated", "networkpolicies"),
    }
    result = outcomes(measure(cluster))
    assert result[(2, "guard_refusal", "none", "none", "none")] == ("fail", "guard_admitted")
    assert result[(2, "rbac_refusal", "none", "none", "none")] == ("fail", "review_allowed")
    assert result[(8, "rbac_policy", "none", "none", "none")] == ("fail", "review_allowed")


def test_reviews_skip_the_system_set_and_the_measuring_identity() -> None:
    cluster = ModelCluster()
    cluster.allowed_reviews = {
        ("system:masters", "pods"),
        ("system:serviceaccount:control:measuring", "pods"),
    }
    # The system set and the measuring identity are not ordinary identities, so no
    # review fails; the only non-pass is the by-design workload_replaced entry.
    result = outcomes(measure(cluster))
    assert not [key for key, (outcome, _) in result.items() if outcome == "fail"], result
    assert result[(2, "rbac_refusal", "none", "none", "none")] == ("pass", "reviews_denied")


def test_a_policy_change_during_the_campaign_fails_case_eight() -> None:
    cluster = ModelCluster()
    cluster.bump_policy_after_reads = 3
    result = outcomes(measure(cluster))
    assert result[(8, "all_policies_present", "none", "none", "none")] == (
        "fail",
        "policy_set_changed",
    )


def test_unsupplied_provider_destinations_are_unavailable_not_pass() -> None:
    layout = replace(LAYOUT, supplied_egress={})
    result = outcomes(measure(ModelCluster(), layout))
    for sub_case in ("instance_metadata", "kubernetes_api", "cloud_network"):
        assert result[(6, sub_case, "ipv4", "none", "runner_policy")] == (
            "unavailable",
            "destination_unknown",
        )
    assert result[(6, "internet", "ipv4", "none", "runner_policy")] == ("pass", "denied")


@pytest.mark.parametrize(
    "changes",
    [
        {"system_set": frozenset()},
        {"system_set": frozenset({Subject("Group", "system:authenticated")})},
        {"eligible_nodes": ()},
        {"families": ("ipv5",)},
        {"release_namespace": "prot"},
        {"targets": (BROKER,)},
    ],
)
def test_layout_refuses_incomplete_inputs(changes: dict[str, Any]) -> None:
    from curie_worker.managed_cni_probe_kit import ProbeKitRefused

    with pytest.raises(ProbeKitRefused):
        replace(LAYOUT, **changes)


def test_wildcard_subjects_refuse() -> None:
    from curie_worker.managed_cni_probe_kit import ProbeKitRefused

    with pytest.raises(ProbeKitRefused):
        Subject("Group", "*")


def test_a_destination_nobody_can_reach_is_unavailable_not_pass() -> None:
    cluster = ModelCluster()
    cluster.dropped = {"8.8.8.8"}
    result = outcomes(measure(cluster))
    assert result[(6, "external_dns", "ipv4", "none", "runner_policy")] == (
        "unavailable",
        "destination_unreached",
    )
    assert result[(6, "internet", "ipv4", "none", "runner_policy")] == ("pass", "denied")


def test_each_target_is_forged_separately_and_each_must_be_refused() -> None:
    cluster = ModelCluster()
    assert outcomes(measure(cluster))[(2, "guard_refusal", "none", "none", "none")] == (
        "pass",
        "guard_refused",
    )
    # Case 2 forges the broker and mediator selector labels; case 7's node-capable
    # dry-runs carry no role label and are not these forgeries.
    roles = sorted(labels[ROLE] for labels in cluster.forgeries if ROLE in labels)
    assert roles == ["broker", "mediator"]
    admitted = ModelCluster()
    admitted.admit_forgery_roles = {"broker"}
    result = outcomes(measure(admitted))
    assert result[(2, "guard_refusal", "none", "none", "none")] == ("fail", "guard_admitted")


def test_a_malformed_binding_subject_is_unavailable_not_a_crash() -> None:
    cluster = ModelCluster()
    cluster.extra_bindings = [
        {"metadata": {"name": "broken"}, "subjects": [{"kind": "ServiceAccount", "name": "x"}]}
    ]
    result = outcomes(measure(cluster))
    for key in (
        (2, "rbac_refusal", "none", "none", "none"),
        (8, "rbac_policy", "none", "none", "none"),
    ):
        assert result[key] == ("unavailable", "review_unavailable")


def test_no_two_senders_share_a_node_while_an_address_window_is_open() -> None:
    cluster = ModelCluster()
    measure(cluster)
    assert cluster.overlaps == []


def test_real_targets_are_probed_on_every_protocol_they_serve() -> None:
    cluster = ModelCluster()
    measure(cluster)
    assert {proto for address, proto in cluster.real_attempts if address == "10.96.0.10"} == {
        "tcp",
        "udp",
    }
    assert {proto for address, proto in cluster.real_attempts if address == "10.96.0.11"} == {"tcp"}


# --- Cases 3, 4, 5 and 7 flow through the model (this slice) ---


def test_an_enforcing_cluster_passes_every_timing_case() -> None:
    result = outcomes(measure(ModelCluster()))
    for sub_case in ("spoof_live", "spoof_terminated"):
        assert result[(3, sub_case, "ipv4", "same_node", "raw_socket")] == ("pass", "denied")
    assert result[(4, "new_pod", "ipv4", "same_node", "protected_unselected")] == ("pass", "denied")
    assert result[(4, "address_reuse", "ipv4", "same_node", "protected_unselected")] == (
        "pass",
        "denied",
    )
    assert result[(5, "policy_changed", "ipv4", "same_node", "retained_peer")] == ("pass", "denied")
    # workload_replaced cannot be a pass from user space (no original sequence state):
    # a non-leaking engine yields unavailable, never pass.
    assert result[(5, "workload_replaced", "ipv4", "same_node", "retained_peer")] == (
        "unavailable",
        "sequence_state_unavailable",
    )
    assert result[(7, "node_origin", "ipv4", "same_node", "node")] == ("pass", "reaches")
    assert result[(7, "rbac_node_capability", "none", "none", "none")] == ("pass", "reviews_denied")
    assert result[(7, "admission_node_capability", "none", "none", "none")] == (
        "pass",
        "guard_refused",
    )
    assert result[(7, "running_node_capability", "none", "none", "none")] == (
        "pass",
        "reviews_denied",
    )


def test_a_spoofed_source_that_reaches_a_twin_fails_case_three() -> None:
    cluster = ModelCluster()
    cluster.spoof_leaks = True
    measurement = measure(cluster)
    assert measurement.refusal == "delivered"
    result = outcomes(measurement)
    assert result[(3, "spoof_live", "ipv4", "same_node", "raw_socket")] == ("fail", "delivered")


def test_unprovoked_reuse_is_unavailable_and_refuses_the_campaign() -> None:
    cluster = ModelCluster()
    cluster.recycle = False  # the terminated address is never handed back out
    measurement = measure(cluster)
    assert measurement.refusal == "reuse_not_provoked"
    result = outcomes(measurement)
    assert result[(4, "address_reuse", "ipv4", "same_node", "protected_unselected")] == (
        "unavailable",
        "reuse_not_provoked",
    )
    assert result[(5, "workload_replaced", "ipv4", "same_node", "retained_peer")] == (
        "unavailable",
        "reuse_not_provoked",
    )


def test_a_retained_flow_after_a_policy_change_fails_case_five() -> None:
    cluster = ModelCluster()
    cluster.retained_leaks = True
    measurement = measure(cluster)
    assert measurement.refusal == "retained_flow"
    result = outcomes(measurement)
    assert result[(5, "policy_changed", "ipv4", "same_node", "retained_peer")] == (
        "fail",
        "retained_flow",
    )


def test_conntrack_resume_after_workload_replace_fails_case_five() -> None:
    # The four-tuple injection from the spoofing template reaches the hold twin when
    # connection tracking retains the flow: a leak, not a pass.
    cluster = ModelCluster()
    cluster.retained_leaks = True
    result = outcomes(measure(cluster))
    assert result[(5, "workload_replaced", "ipv4", "same_node", "retained_peer")] == (
        "fail",
        "retained_flow",
    )


def test_a_dead_listener_after_the_policy_change_is_unavailable_not_pass() -> None:
    # Without the post-change liveness control, a silent listener would read as a
    # pass; the control makes it unavailable instead.
    cluster = ModelCluster()
    cluster.drop_liveness = True
    result = outcomes(measure(cluster))
    assert result[(5, "policy_changed", "ipv4", "same_node", "retained_peer")] == (
        "unavailable",
        "control_failed",
    )


def test_node_local_traffic_from_a_tunnel_address_is_unavailable() -> None:
    cluster = ModelCluster()
    cluster.node_local_tunnel = True
    result = outcomes(measure(cluster))
    assert result[(7, "node_origin", "ipv4", "same_node", "node")] == (
        "unavailable",
        "node_probe_unavailable",
    )


def test_admission_that_admits_a_node_capable_pod_fails_case_seven() -> None:
    cluster = ModelCluster()
    cluster.admit_node_capable = True  # the guard does not fence node-capable fields
    result = outcomes(measure(cluster))
    assert result[(7, "admission_node_capability", "none", "none", "none")] == (
        "fail",
        "guard_admitted",
    )


def test_node_local_blocked_is_a_recorded_fact_not_a_refusal() -> None:
    cluster = ModelCluster()
    cluster.node_local_reaches = False
    result = outcomes(measure(cluster))
    # node_origin blocked is a recorded pass, not a refusal source; it does not
    # drive the campaign refusal (that is the by-design workload_replaced entry).
    assert result[(7, "node_origin", "ipv4", "same_node", "node")] == ("pass", "blocked")
    assert (7, "node_origin", "ipv4", "same_node", "node") not in {
        key for key, (outcome, _) in result.items() if outcome != "pass"
    }


def test_node_capable_rights_when_node_local_reaches_force_a_separate_domain() -> None:
    cluster = ModelCluster()
    # A privileged pod of an ordinary identity on an eligible node.
    cluster.extra_pods = [
        {
            "metadata": {"name": "intruder", "namespace": "kube-system"},
            "spec": {
                "nodeName": "n1",
                "containers": [{"securityContext": {"privileged": True}}],
            },
            "status": {},
        }
    ]
    measurement = measure(cluster)
    assert measurement.refusal == "separate_authority_domain_required"
    result = outcomes(measurement)
    assert result[(7, "running_node_capability", "none", "none", "none")] == (
        "fail",
        "node_capable_pod",
    )


def test_nodes_proxy_allowed_fails_rbac_node_capability() -> None:
    cluster = ModelCluster()
    cluster.allowed_reviews = {("alice", "nodes")}
    result = outcomes(measure(cluster))
    assert result[(7, "rbac_node_capability", "none", "none", "none")] == ("fail", "review_allowed")


def _run_for(cluster: ModelCluster):  # type: ignore[no-untyped-def]
    from curie_worker.managed_cni_cases import _Run

    return _Run(cluster, LAYOUT, cluster.clock, cluster.sleep)


def test_address_reuse_with_no_sender_record_is_unavailable_not_pass() -> None:
    # Item 3: address_reuse parses the reusing pod's own client result; a missing
    # sender record cannot read as a clean denial. Called directly so the missing
    # record does not gate an earlier case.
    from curie_worker.managed_cni_cases import _twins
    from curie_worker.managed_cni_timing import _address_reuse

    cluster = ModelCluster()
    cluster.sender_result_missing = True
    run = _run_for(cluster)

    async def go() -> Any:
        twins = await _twins(run)
        return await _address_reuse(run, twin=twins["broker"], broker=BROKER, family="ipv4")

    entry = asyncio.run(go())
    assert (entry.outcome, entry.reason) == ("unavailable", "phase_timeout")


def test_running_node_capability_sees_a_privileged_init_container() -> None:
    cluster = ModelCluster()
    cluster.extra_pods = [
        {
            "metadata": {"name": "intruder", "namespace": "kube-system"},
            "spec": {
                "nodeName": "n1",
                "containers": [{}],
                "initContainers": [{"securityContext": {"privileged": True}}],
            },
            "status": {},
        }
    ]
    assert outcomes(measure(cluster))[(7, "running_node_capability", "none", "none", "none")] == (
        "fail",
        "node_capable_pod",
    )


def test_case_five_listeners_land_in_the_probe_namespace() -> None:
    # Item 8: the retained listener uses the probe_listener template in probe-a, not
    # the protected-namespace twin template.
    cluster = ModelCluster()
    measure(cluster)
    assert ("probe-a", "probe_listener") in cluster.created_templates
    assert ("probe-a", "twin") not in cluster.created_templates


def test_spoof_terminated_refuses_a_stale_address() -> None:
    # Item 9: an address confirmed absent more than 30 s earlier no longer measures a
    # recently terminated one.
    cluster = ModelCluster()
    cluster.slow_absence = True
    result = outcomes(measure(cluster))
    assert result[(3, "spoof_terminated", "ipv4", "same_node", "raw_socket")] == (
        "unavailable",
        "address_reassigned",
    )


def test_case_five_waits_for_a_sender_whose_container_is_still_starting() -> None:
    # A real API server refuses the log of a pod whose container is still being
    # created. Case 5 launches its retained sender without waiting for a phase, so
    # that refusal means "not yet", not a timeout; the established flow must still
    # be measured and an enforcing cluster must pass.
    cluster = ModelCluster()
    cluster.container_start_s = 3.0
    result = outcomes(measure(cluster))
    for placement in ("same_node", "cross_node"):
        assert result[(5, "policy_changed", "ipv4", placement, "retained_peer")] == (
            "pass",
            "denied",
        )
        assert result[(5, "workload_replaced", "ipv4", placement, "retained_peer")] == (
            "unavailable",
            "sequence_state_unavailable",
        )


def test_a_retained_sender_deleted_before_it_starts_is_unavailable() -> None:
    # The wait ends when the sender pod is gone, not only at the phase deadline.
    from curie_worker.managed_cni_timing import _await_log

    cluster = ModelCluster()
    cluster.container_start_s = 30.0
    run = _run_for(cluster)

    async def go() -> tuple[bool, float]:
        created = await cluster.create_pod("probe-a", _retained_document())
        uid = created["metadata"]["uid"]
        await cluster.delete_pod("probe-a", "s", uid)
        start = cluster.now
        found = await _await_log(run, "probe-a", "s", uid, "established", start + 120)
        return found, cluster.now - start

    found, waited = asyncio.run(go())
    assert found is False and waited < 2


def _retained_document() -> dict[str, Any]:
    from curie_worker.managed_cni_probe_programs import probe_pod

    return probe_pod(
        template="retained_sender", name="s", namespace="probe-a", node="n1", campaign="c1",
        role_labels={}, argument={"addr": "10.0.0.1", "port": 1, "nonces": ["a" * 32],
                                  "interval_ms": 100, "timeout_ms": 2000},
    )


def test_policy_changed_with_no_post_heartbeat_is_unavailable() -> None:
    # Item 2: without a heartbeat sent past the bound, retention cannot be judged.
    cluster = ModelCluster()
    cluster.retained_no_post = True
    result = outcomes(measure(cluster))
    assert result[(5, "policy_changed", "ipv4", "same_node", "retained_peer")] == (
        "unavailable",
        "phase_timeout",
    )


def test_node_origin_without_a_readiness_result_is_unavailable() -> None:
    # Item 4: the kubelet-probe pod exists but its readiness probe never ran, so a
    # silent twin cannot be read as blocked.
    cluster = ModelCluster()
    cluster.kubelet_readiness_absent = True
    result = outcomes(measure(cluster))
    assert result[(7, "node_origin", "ipv4", "same_node", "node")] == (
        "unavailable",
        "node_probe_unavailable",
    )


def test_an_injector_that_never_sent_makes_workload_replaced_unavailable() -> None:
    # Item 1: the spoof injector's result is parsed; if it did not send, the entry is
    # unavailable rather than a vacuous pass. Called directly so it does not gate case 3.
    from curie_worker.managed_cni_cases import _twins
    from curie_worker.managed_cni_timing import _workload_replaced

    cluster = ModelCluster()
    cluster.spoof_send_fails = True
    run = _run_for(cluster)

    async def go() -> Any:
        twins = await _twins(run)
        return await _workload_replaced(
            run, twin=twins["broker"], broker=BROKER, family="ipv4", placement="same_node"
        )

    entry = asyncio.run(go())
    assert (entry.outcome, entry.reason) == ("unavailable", "transport_unavailable")
