"""Fresh case orchestration against modeled external Kubernetes, never CNI proof."""

from __future__ import annotations

import asyncio
import copy
import importlib
import importlib.util
import json
from dataclasses import replace
from typing import Any

import pytest
from curie_worker.managed_cni_cases import _cleanup, _Pod

from apps.worker.tests.test_managed_cni_fresh_lifecycle import PolicyAPI, run

ROLE = "curietech.ai/cni-fresh-role"


class FreshAPI(PolicyAPI):
    """External Pod programs/network/policy behavior, isolated for every test."""

    def __init__(self, **faults: Any) -> None:
        super().__init__(uid="unused")
        self.pods: dict[tuple[str, str], dict[str, Any]] = {}
        self.policies: dict[str, dict[str, Any]] = {}
        self.protected = [
            {
                "metadata": {
                    "name": "broker",
                    "namespace": "prot",
                    "uid": "broker-policy",
                    "resourceVersion": "1",
                },
                "spec": {
                    "podSelector": {"matchLabels": {"role": "broker"}},
                    "policyTypes": ["Ingress"],
                    "ingress": [
                        {
                            "from": [{"podSelector": {"matchLabels": {"role": "client"}}}],
                            "ports": [{"protocol": "TCP", "port": 16379}],
                        }
                    ],
                },
            }
        ]
        if faults.get("extra_selector"):
            self.protected.append(
                {
                    "metadata": {
                        "name": "extra",
                        "namespace": "prot",
                        "uid": "extra",
                        "resourceVersion": "1",
                    },
                    "spec": {
                        "podSelector": {"matchLabels": {"other": "real-only"}},
                        "policyTypes": ["Ingress"],
                        "ingress": [{}],
                    },
                }
            )
        if faults.get("named_port"):
            self.protected[0]["spec"]["ingress"][0]["ports"][0]["port"] = "broker"
        self.logs: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.arguments: dict[tuple[str, str], dict[str, Any]] = {}
        self.poisoned: set[tuple[str, str]] = set()
        self.free: dict[str, list[str]] = {}
        self.count, self.port = 10, 44000
        self.started: dict[tuple[str, str], float] = {}
        self.ip_prefix = "10.0.0"
        self.faults = faults
        self.creates: list[dict[str, Any]] = []
        self.creation_times: dict[str, float] = {}
        self.triggers: list[tuple[float, str, str]] = []

    def _ticks(self, key: tuple[str, str]) -> None:
        rows = self.logs[key]
        ticks = [r["elapsed_ms"] for r in rows if r["ev"] == "tick"]
        last = ticks[-1] if ticks else -500
        while last + 500 <= round((self.now - self.started[key]) * 1000):
            last += 500
            rows.append(
                {
                    "ev": "tick",
                    "elapsed_ms": last,
                    "sampled": not self.faults.get("unsampled", False),
                    "overflow": 0,
                }
            )

    def _owner(self, address: str) -> tuple[str, str] | None:
        return next(
            (key for key, p in self.pods.items() if p["status"]["podIPs"][0]["ip"] == address), None
        )

    def _allowed(self, source: tuple[str, str], dest: tuple[str, str] | None, port: int) -> bool:
        src = self.pods[source]
        if dest is None or dest[0] == "prot":
            return source[0] == "prot" and all(
                src["metadata"]["labels"].get(k) == v
                for k, v in self.faults.get("allowed_labels", {"role": "client"}).items()
            )
        dst = self.pods[dest]
        policies = [
            p
            for p in self.policies.values()
            if all(
                dst["metadata"]["labels"].get(k) == v
                for k, v in p["spec"]["podSelector"].get("matchLabels", {}).items()
            )
        ]
        if not policies:
            return True
        return source[0] == dest[0] and any(
            all(
                src["metadata"]["labels"].get(k) == v
                for k, v in peer["podSelector"]["matchLabels"].items()
            )
            and any(item["port"] == port and item["protocol"] == "TCP" for item in rule["ports"])
            for policy in policies
            for rule in policy["spec"].get("ingress", [])
            for peer in rule["from"]
        )

    def _attempt(
        self, key: tuple[str, str], target: dict[str, Any], nonce: str, *, denial: bool = False
    ) -> dict[str, Any]:
        self.port += 1
        owner = self._owner(target["address"])
        allowed = self._allowed(key, owner, target["port"])
        delivery = allowed or (denial and self.faults.get("listener_leak", False))
        connected = allowed or (denial and self.faults.get("client_leak", False))
        row = {
            "ev": "attempt",
            "target": target["target"],
            "source": self.pods[key]["status"]["podIPs"][0]["ip"],
            "source_port": self.port,
            "destination": target["address"],
            "port": target["port"],
            "nonce": nonce,
            "outcome": "connected" if connected else "timeout",
            "echo": connected
            and target["echo"]
            and not (
                self.faults.get("one_way", False) and self.arguments[key].get("mode") == "control"
            ),
        }
        if delivery and owner:
            if denial and self.faults.get("malformed_listener"):
                self.poisoned.add(owner)
            self._ticks(owner)
            self.logs[owner].append(
                {
                    "ev": "data",
                    **{
                        k: row[k] for k in ("source", "source_port", "destination", "port", "nonce")
                    },
                }
            )
        if denial and self.faults.get("unknown_error"):
            row["outcome"], row["echo"] = "error", False
        return row

    def _finish(self, key: tuple[str, str], challenge: str) -> None:
        cfg = self.arguments[key]
        self.logs[key].append({"ev": "challenge", "challenge": challenge})
        if cfg["mode"] == "deny" and self.faults.get("missing_attempts"):
            self.pods[key]["status"]["phase"] = "Succeeded"
            return
        for index, target in enumerate(cfg["targets"]):
            for ordinal in range(5 if cfg["mode"] == "deny" else 1):
                self.logs[key].append(
                    self._attempt(
                        key,
                        target,
                        challenge + f"{index:02x}{ordinal:02x}",
                        denial=cfg["mode"] == "deny",
                    )
                )
        self.logs[key].append({"ev": "done", "ok": True})
        if cfg["mode"] == "deny" and self.faults.get("malformed_tail"):
            self.logs[key].append({"ev": "unknown"})
        self.pods[key]["status"]["phase"] = "Succeeded"

    async def create_pod(self, namespace: str, document: dict[str, Any]) -> dict[str, Any]:
        self.count += 1
        pod = copy.deepcopy(document)
        self.creates.append(copy.deepcopy(document))
        key = namespace, pod["metadata"]["name"]
        pod["metadata"]["uid"] = str(self.count)
        self.creation_times[str(self.count)] = self.now
        node = pod["spec"]["nodeName"]
        pool = self.free.setdefault(node, [])
        ip = (
            pool.pop(0)
            if pool and not self.faults.get("no_reuse")
            else f"{self.ip_prefix}.{self.count}"
        )
        pod["status"] = {
            "phase": "Running",
            "podIPs": [{"ip": ip}],
            "containerStatuses": [
                {
                    "name": pod["spec"]["containers"][0]["name"],
                    "restartCount": 0,
                    "containerID": f"containerd://{self.count}",
                    "imageID": "sha256:fixed",
                }
            ],
        }
        self.pods[key], self.logs[key] = pod, []
        self.started[key] = self.now
        cfg = json.loads(pod["spec"]["containers"][0]["command"][4])
        self.arguments[key] = cfg
        if "targets" in cfg:
            setup = (
                self._attempt(key, cfg["targets"][0], cfg["setup_nonce"] + "ffff")
                if cfg["setup"]
                else None
            )
            excluded = [*cfg["excluded_ports"], *([] if setup is None else [setup["source_port"]])]
            self.logs[key].append(
                {
                    "ev": "ready",
                    "mode": cfg["mode"],
                    "source": ip,
                    "setup": setup,
                    "targets": cfg["targets"],
                    "excluded_ports": excluded,
                }
            )
            if cfg["mode"] == "control":
                if self.faults.get("late_control"):
                    self.now += 121
                self._finish(key, cfg["challenge"])
        elif "challenge" in cfg:
            owner = self._owner(cfg["address"])
            ack = owner is not None and self._allowed(key, owner, 42001)
            if ack:
                assert owner is not None
                challenge = "b" * 32 if self.faults.get("wrong_challenge") else cfg["challenge"]
                self.triggers.append((self.now, self.pods[owner]["metadata"]["uid"], challenge))
                self._finish(owner, challenge)
            self.logs[key] = [
                {"ev": "trigger", "challenge": cfg["challenge"], "acknowledged": ack},
                {"ev": "done", "ok": ack},
            ]
            pod["status"]["phase"] = "Succeeded"
        else:
            self.logs[key].append({"ev": "ready", "port": cfg["port"]})
            self._ticks(key)
        return copy.deepcopy(pod)

    async def read_pod(self, namespace: str, name: str) -> dict[str, Any] | None:
        key = namespace, name
        pod = copy.deepcopy(self.pods.get(key))
        if pod and self.now < self.started[key] + self.faults.get("startup_delay", 0):
            pod["status"].update(phase="Pending", podIPs=[], containerStatuses=[])
        return pod

    async def pod_log(self, namespace: str, name: str) -> str:
        key = namespace, name
        if "port" in self.arguments[key]:
            self._ticks(key)
        rows = [*self.logs[key], *([{"ev": "unknown"}] if key in self.poisoned else [])]
        return "\n".join(json.dumps({"v": 2, "seq": i, **r}) for i, r in enumerate(rows))

    async def delete_pod(self, namespace: str, name: str, uid: str) -> None:
        pod = self.pods.get((namespace, name))
        if pod and pod["metadata"]["uid"] == uid:
            self.free.setdefault(pod["spec"]["nodeName"], []).append(
                pod["status"]["podIPs"][0]["ip"]
            )
            del self.pods[namespace, name]

    async def create_network_policy(
        self, namespace: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        assert namespace == "probe-a"
        pod = copy.deepcopy(document)
        pod["metadata"].update(uid="np-" + document["metadata"]["name"], resourceVersion="1")
        self.policies[pod["metadata"]["name"]] = pod
        return copy.deepcopy(pod)

    async def delete_network_policy(
        self, namespace: str, name: str, uid: str | None = None
    ) -> None:
        assert namespace == "probe-a" and uid
        policy = self.policies.get(name)
        if policy and policy["metadata"]["uid"] == uid:
            del self.policies[name]

    async def list_items(self, collection: str) -> list[dict[str, Any]]:
        if collection == "networkpolicies:prot":
            return copy.deepcopy(self.protected)
        assert collection == "networkpolicies:probe-a"
        return copy.deepcopy(list(self.policies.values()))


def measure(cluster: FreshAPI, *, single_node: bool = False) -> tuple[list[Any], Any]:
    name = "curie_worker.managed_cni_fresh_cases"
    assert importlib.util.find_spec(name) is not None, "fresh case coordinator is not implemented"
    active = run(cluster, policies=False)
    active.layout = replace(
        active.layout, eligible_nodes=("node-a",) if single_node else ("node-a", "node-b")
    )
    anchor = _Pod("prot", "broker-twin", "anchor", "node-a", ("10.0.0.1",))

    async def go() -> list[Any]:
        try:
            return await importlib.import_module(name).case_five_fresh(active, {"broker": anchor})
        finally:
            assert await _cleanup(active) == []

    result = asyncio.run(go())
    assert not cluster.pods and not cluster.policies
    return result, active


def test_case_five_confirms_both_triggers_and_denies_every_required_fresh_flow() -> None:
    entries, active = measure(FreshAPI())
    assert len(entries) == 4 and all(item.outcome == "pass" for item in entries), active.evidence


@pytest.mark.parametrize("fault", ["client_leak", "listener_leak"])
def test_a_positive_new_flow_fails_even_if_later_control_is_unavailable(fault: str) -> None:
    entries, _ = measure(FreshAPI(**{fault: True, "one_way": True}))
    assert entries and all(item.outcome == "fail" for item in entries)


@pytest.mark.parametrize(
    "fault", ["one_way", "unsampled", "unknown_error", "late_control", "wrong_challenge"]
)
def test_missing_causal_control_or_complete_denial_is_unavailable(fault: str) -> None:
    entries, _ = measure(FreshAPI(**{fault: True}))
    assert entries and all(item.outcome == "unavailable" for item in entries)


def test_unprovoked_reuse_does_not_shrink_or_pass_the_matrix() -> None:
    entries, _ = measure(FreshAPI(no_reuse=True), single_node=True)
    assert len(entries) == 4
    assert all(item.outcome == "unavailable" for item in entries if item.placement == "cross_node")
    assert all(
        item.reason == "reuse_not_provoked"
        for item in entries
        if item.sub_case == "workload_replaced" and item.placement == "same_node"
    )


def test_original_sender_and_fresh_control_are_causally_bound() -> None:
    cluster = FreshAPI()
    entries, active = measure(cluster)
    assert all(e.outcome == "pass" for e in entries)
    assert len(cluster.triggers) == 4 and len({item[2] for item in cluster.triggers}) == 4
    for entry in active.evidence:
        evidence = entry["evidence"]
        assert evidence["sender"]["uid"] in {item[1] for item in cluster.triggers}
        trigger_at = cluster.creation_times[evidence["trigger_pod"]["uid"]]
        control_at = cluster.creation_times[evidence["control_pod"]["uid"]]
        assert control_at >= trigger_at + 5
        assert evidence["sender"]["binding_digest"] and evidence["listener"]["container_id"]
        setup, attempts, controls = evidence["setup"], evidence["attempts"], evidence["control"]
        assert all(a["source_port"] != setup["source_port"] for a in attempts)
        assert len({a["source_port"] for a in attempts + controls}) == len(attempts + controls)
        assert all(a["nonce"].startswith(evidence["trigger_nonce"]) for a in attempts)
        assert all(not a["nonce"].startswith(evidence["trigger_nonce"]) for a in controls)
        if entry["sub_case"] == "workload_replaced":
            assert evidence["terminated_original"]["uid"] != evidence["sender"]["uid"]
            assert evidence["terminated_original"]["addresses"] == evidence["sender"]["addresses"]
        else:
            assert setup["source"] == attempts[0]["source"]
    assert all(p["spec"]["automountServiceAccountToken"] is False for p in cluster.creates)


@pytest.mark.parametrize("fault", ["extra_selector", "named_port"])
def test_unverified_protected_policy_shapes_never_qualify_a_twin(fault: str) -> None:
    entries, _ = measure(FreshAPI(**{fault: True}))
    assert entries and all(item.outcome == "unavailable" for item in entries)


def test_supported_default_deny_and_allowance_are_copied_from_actual_policy() -> None:
    cluster = FreshAPI()
    # Kubernetes NetworkPolicySpec.Ingress uses json:"ingress,omitempty".
    # https://github.com/kubernetes/api/blob/v0.35.0/networking/v1/types.go
    cluster.protected.append(
        {
            "metadata": {
                "name": "deny",
                "namespace": "prot",
                "uid": "deny",
                "resourceVersion": "1",
            },
            "spec": {"podSelector": {}, "policyTypes": ["Ingress"]},
        }
    )
    entries, _ = measure(cluster)
    assert all(item.outcome == "pass" for item in entries)


def test_reusing_a_pretrigger_nonce_never_qualifies_a_case(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("curie_worker.managed_cni_fresh_cases")
    monkeypatch.setattr(module, "_nonce", lambda: "a" * 32)
    entries, _ = measure(FreshAPI())
    assert entries and all(item.outcome == "unavailable" for item in entries)


def test_a_known_handshake_is_not_erased_by_later_malformed_log_data() -> None:
    entries, _ = measure(FreshAPI(client_leak=True, malformed_tail=True))
    assert entries and all(item.outcome == "fail" for item in entries)


def test_egress_selection_cannot_make_real_and_twin_delivery_differ() -> None:
    cluster = FreshAPI()
    cluster.protected.append(
        {
            "metadata": {
                "name": "return-filter",
                "namespace": "prot",
                "uid": "return",
                "resourceVersion": "1",
            },
            "spec": {
                "podSelector": {"matchLabels": {"other": "real-only"}},
                "policyTypes": ["Egress"],
                "egress": [],
            },
        }
    )
    entries, _ = measure(cluster)
    assert entries and all(item.outcome == "unavailable" for item in entries)


def test_unconfirmed_cleanup_stops_new_work_and_reports_every_required_entry() -> None:
    class StickyAPI(FreshAPI):
        stuck = True

        async def delete_pod(self, namespace: str, name: str, uid: str) -> None:
            if not self.stuck:
                await super().delete_pod(namespace, name, uid)

    cluster = StickyAPI()
    active = run(cluster, policies=False)
    active.layout = replace(active.layout, eligible_nodes=("node-a", "node-b"))
    anchor = _Pod("prot", "anchor", "anchor", "node-a", ("10.0.0.1",))

    async def go() -> list[Any]:
        try:
            module = importlib.import_module("curie_worker.managed_cni_fresh_cases")
            return await module.case_five_fresh(active, {"broker": anchor})
        finally:
            cluster.stuck = False
            assert await _cleanup(active) == []

    entries = asyncio.run(go())
    assert len(entries) == 4 and all(
        (e.outcome, e.reason) == ("unavailable", "cleanup_unconfirmed") for e in entries
    )
    assert len(cluster.triggers) == 1
    assert not cluster.pods and not cluster.policies


def test_listener_delivery_is_not_erased_by_a_later_malformed_row() -> None:
    entries, _ = measure(FreshAPI(listener_leak=True, malformed_listener=True))
    assert entries and all(item.outcome == "fail" for item in entries)
