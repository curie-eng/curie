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
from curie_worker.managed_cni_probe_kit import Measurement, accepts

ROLE = "curietech.ai/policy-role"
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
        self.real_attempts: list[tuple[str, str]] = []
        # Per node, the model time until which a sender's address and window are in use.
        self.busy_until: dict[str, float] = {}
        self.overlaps: list[str] = []
        self.extra_bindings: list[dict[str, Any]] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)  # let other probes interleave, as real waits do

    def _ms(self) -> int:
        return int(self.now * 1000)

    async def create_pod(self, namespace: str, document: dict[str, Any]) -> dict[str, Any]:
        name = document["metadata"]["name"]
        assert (namespace, name) not in self.pods
        self.now += 1.0  # pod start latency
        self.created += 1
        ip = f"10.0.{self.next_ip // 250}.{self.next_ip % 250 + 1}"
        self.next_ip += 1
        uid = f"uid-{self.created}"
        command = document["spec"]["containers"][0]["command"]
        pod = {
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": uid,
                "labels": document["metadata"]["labels"],
            },
            "spec": {"nodeName": document["spec"]["nodeName"]},
            "status": {"phase": "Running", "podIPs": [{"ip": ip}]},
            "program": "listener" if "accept" in command[3] else "sender",
            "argument": json.loads(command[4]),
        }
        self.pods[(namespace, name)] = pod
        self.ip_owner[ip] = (namespace, name)
        self.logs[(namespace, name)] = []
        if pod["program"] == "listener":
            self.logs[(namespace, name)].append({"ev": "ready", "t": self._ms()})
        else:
            self._send(pod)
            pod["status"]["phase"] = "Succeeded"
        return {"metadata": {"uid": uid}}

    def _admits(self, sender: dict[str, Any], listener: dict[str, Any], port: int) -> bool:
        if not self.enforcing:
            return True
        labels = sender["metadata"]["labels"]
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
        template = sender["metadata"]["labels"]["curietech.ai/managed-cni-probe-template"]
        if template in ("probe_sender", "protected_probe", "release_probe"):
            self.busy_until[node] = self.now + 5.0
        self.logs[(sender["metadata"]["namespace"], sender["metadata"]["name"])].append(
            {"ev": "result", "start": start, "end": self._ms(), "attempts": results}
        )

    async def read_pod(self, namespace: str, name: str) -> dict[str, Any] | None:
        return self.pods.get((namespace, name))

    async def pod_log(self, namespace: str, name: str) -> str:
        pod = self.pods[(namespace, name)]
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
            del self.pods[(namespace, name)]

    async def dry_run_create_pod(
        self, namespace: str, document: dict[str, Any]
    ) -> tuple[int | None, str]:
        assert "curietech.ai/managed-cni-probe-template" not in document["metadata"]["labels"]
        labels = document["metadata"]["labels"]
        self.forgeries.append(labels)
        if labels.get(ROLE) in self.admit_forgery_roles:
            return (201, "")
        return self.dry_run

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


def test_enforcing_two_node_cluster_passes_every_required_entry() -> None:
    cluster = ModelCluster()
    measurement = measure(cluster)
    assert measurement.refusal is None, outcomes(measurement)
    assert accepts(measurement.entries, required_entries(LAYOUT))
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
    assert measure(cluster).refusal is None


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
    assert measure(cluster).refusal is None
    roles = sorted(labels[ROLE] for labels in cluster.forgeries)
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
