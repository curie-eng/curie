"""Owned probe policy cleanup through the actual case lifecycle boundary."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from curie_worker.managed_cni_cases import Destination, ProbeLayout, Subject, Target, _cleanup, _Run
from curie_worker.managed_cni_probe_kit import ProbeKitRefused


class PolicyAPI:
    def __init__(self, *, uid: str, delete_fails: bool = False, list_fails: bool = False) -> None:
        self.item: dict[str, Any] | None = {
            "metadata": {"name": "owned", "namespace": "probe-a", "uid": uid}
        }
        self.delete_fails, self.list_fails = delete_fails, list_fails
        self.now = 0.0

    async def delete_network_policy(
        self, namespace: str, name: str, uid: str | None = None
    ) -> None:
        assert (namespace, name) == ("probe-a", "owned")
        if self.delete_fails:
            raise ProbeKitRefused
        if self.item and (uid is None or self.item["metadata"]["uid"] == uid):
            self.item = None

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        assert collection == "networkpolicies:probe-a"
        if self.list_fails:
            return None
        return [] if self.item is None else [self.item]

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


def run(client: PolicyAPI, *, policies: bool = True) -> _Run:
    layout = ProbeLayout(
        campaign="cleanup",
        protected_namespace="prot",
        probe_namespace="probe-a",
        foreign_namespace="probe-foreign",
        release_namespace="release",
        eligible_nodes=("node-a",),
        families=("ipv4",),
        targets=(
            Target(
                "broker",
                {"role": "broker"},
                {"role": "client"},
                16379,
                ("tcp",),
                16380,
                ("10.0.0.1",),
            ),
            Target(
                "mediator",
                {"role": "mediator"},
                {"role": "mediator-client"},
                18443,
                ("tcp",),
                18444,
                ("10.0.0.2",),
            ),
        ),
        runner_labels={"role": "runner"},
        declared_peers=(Destination("10.0.0.9", 18080, "tcp"),),
        supplied_egress={},
        guard_names=("guard.example",),
        system_set=frozenset({Subject("Group", "system:masters")}),
        excluded=frozenset(),
        provider_policy_collections=(),
        policy_agent_objects=(),
    )
    return _Run(
        client,
        layout,
        lambda: client.now,
        client.sleep,
        created_policies=[("probe-a", "owned", "original")] if policies else [],
        monotonic=lambda: client.now,
    )  # type: ignore[arg-type]


def test_policy_cleanup_preserves_a_foreign_same_name_replacement() -> None:
    client = PolicyAPI(uid="replacement")
    assert asyncio.run(_cleanup(run(client))) == []
    assert client.item is not None and client.item["metadata"]["uid"] == "replacement"


@pytest.mark.parametrize("list_fails", [False, True])
def test_policy_cleanup_cannot_pass_without_independent_absence(list_fails: bool) -> None:
    client = PolicyAPI(uid="original", delete_fails=True, list_fails=list_fails)
    leftovers = asyncio.run(_cleanup(run(client)))
    assert leftovers and client.item is not None


class AmbiguousAPI(PolicyAPI):
    async def create_pod(self, namespace: str, document: dict[str, Any]) -> dict[str, Any]:
        self.uncertain = {"metadata": {**document["metadata"], "uid": "unconfirmed-pod"}}
        raise ProbeKitRefused

    async def create_network_policy(
        self, namespace: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        self.uncertain = {"metadata": {**document["metadata"], "uid": "unconfirmed-policy"}}
        raise ProbeKitRefused


def test_ambiguous_pod_creation_never_becomes_empty_successful_cleanup() -> None:
    from curie_worker.managed_cni_cases import _start

    client = AmbiguousAPI(uid="foreign")
    active = run(client, policies=False)
    with pytest.raises(ProbeKitRefused):
        asyncio.run(
            _start(
                active,
                template="probe_listener",
                namespace="probe-a",
                node="node-a",
                labels={},
                argument={"tcp": [18080], "udp": []},
                finished=False,
            )
        )
    assert asyncio.run(_cleanup(active))
    assert client.uncertain["metadata"]["uid"] == "unconfirmed-pod"


def test_ambiguous_policy_creation_never_becomes_empty_successful_cleanup() -> None:
    from curie_worker.managed_cni_fresh_cases import _Case

    client = AmbiguousAPI(uid="foreign")
    active = run(client, policies=False)
    with pytest.raises(ProbeKitRefused):
        case = _Case(active, "ipv4", "node-a", active.layout.target("broker"))
        asyncio.run(case.create_policy("owned", {"role": "broker"}, {"role": "client"}, 16379))
    assert asyncio.run(_cleanup(active))
    assert client.uncertain["metadata"]["uid"] == "unconfirmed-policy"


def test_owned_policy_cleanup_confirms_absence() -> None:
    client = PolicyAPI(uid="original")
    assert asyncio.run(_cleanup(run(client))) == []
    assert client.item is None


@pytest.mark.parametrize("metadata", [None, {}, {"uid": ""}, {"uid": True}])
def test_malformed_policy_inventory_never_proves_absence(metadata: Any) -> None:
    client = PolicyAPI(uid="original", delete_fails=True)
    client.item = {"metadata": metadata}
    assert asyncio.run(_cleanup(run(client)))


class StartingAPI(PolicyAPI):
    async def create_pod(self, namespace: str, document: dict[str, Any]) -> dict[str, Any]:
        import copy

        self.pod = copy.deepcopy(document)
        self.pod["metadata"]["uid"] = "fresh-pod"
        self.pod["status"] = {
            "phase": "Running",
            "podIPs": [{"ip": "10.0.0.8"}],
            "containerStatuses": [
                {
                    "name": document["spec"]["containers"][0]["name"],
                    "containerID": "containerd://one",
                    "imageID": "sha256:one",
                    "restartCount": 0,
                }
            ],
        }
        return self.pod

    async def read_pod(self, namespace: str, name: str) -> dict[str, Any]:
        return self.pod

    async def pod_log(self, namespace: str, name: str) -> str:
        import json

        self.now += getattr(self, "log_delay", 0)
        rows = [
            {"ev": "ready", "port": 18080},
            {"ev": "tick", "elapsed_ms": 0, "sampled": True, "overflow": 0},
        ]
        return "\n".join(json.dumps({"v": 2, "seq": i, **row}) for i, row in enumerate(rows))


def start_fresh(client: StartingAPI, *, deadline: float | None = None) -> Any:
    from curie_worker.managed_cni_cases import _start

    return asyncio.run(
        _start(
            run(client, policies=False),
            template="fresh_probe_listener",
            namespace="probe-a",
            node="node-a",
            labels={},
            argument={"family": "ipv4", "port": 18080},
            finished=False,
            deadline=deadline,
        )
    )


def test_actual_fresh_listener_readiness_uses_its_closed_v2_records() -> None:
    client = StartingAPI(uid="unused")
    pod = start_fresh(client)
    assert pod is not None and pod.uid == "fresh-pod" and pod.addresses == ("10.0.0.8",)


def test_readiness_response_after_enclosing_deadline_never_succeeds() -> None:
    client = StartingAPI(uid="unused")
    client.log_delay = 3
    assert start_fresh(client, deadline=2) is None


@pytest.mark.parametrize("change", ["restart", "sidecar", "command", "node"])
def test_fresh_readiness_refuses_changed_admitted_workload(change: str) -> None:
    class MutatedAPI(StartingAPI):
        async def read_pod(self, namespace: str, name: str) -> dict[str, Any]:
            pod = await super().read_pod(namespace, name)
            if change == "restart":
                pod["status"]["containerStatuses"][0]["restartCount"] = 1
            elif change == "sidecar":
                pod["spec"]["containers"].append({"name": "injected"})
            elif change == "command":
                pod["spec"]["containers"][0]["command"] = ["other"]
            else:
                pod["spec"]["nodeName"] = "node-other"
            return pod

    with pytest.raises(ProbeKitRefused):
        start_fresh(MutatedAPI(uid="unused"))


def test_cleanup_uses_one_monotonic_budget_before_any_delete() -> None:
    class SlowAPI(PolicyAPI):
        def __init__(self) -> None:
            super().__init__(uid="original")
            self.deletes: list[str] = []
            self.reads = 0

        async def delete_network_policy(
            self, namespace: str, name: str, uid: str | None = None
        ) -> None:
            self.deletes.append(name)
            self.now += 61

        async def list_items(self, collection: str) -> list[dict[str, Any]]:
            self.reads += 1
            return []

    client = SlowAPI()
    active = run(client)
    active.clock = lambda: -client.now  # backward wall-clock motion cannot extend cleanup
    active.created_policies = [("probe-a", f"owned-{i}", f"uid-{i}") for i in range(3)]
    leftovers = asyncio.run(_cleanup(active))
    assert len(leftovers) == 3
    assert len(client.deletes) <= 2 and client.reads == 0


def test_fresh_pod_disappearance_during_readiness_refuses_immediately() -> None:
    class GoneAPI(StartingAPI):
        async def read_pod(self, namespace: str, name: str) -> Any:
            return None

    client = GoneAPI(uid="unused")
    with pytest.raises(ProbeKitRefused):
        start_fresh(client)
    assert client.now == 0


def test_late_status_response_does_not_start_another_log_request() -> None:
    class LateAPI(StartingAPI):
        reads = 0

        async def read_pod(self, namespace: str, name: str) -> dict[str, Any]:
            self.now += 3
            return await super().read_pod(namespace, name)

        async def pod_log(self, namespace: str, name: str) -> str:
            self.reads += 1
            return await super().pod_log(namespace, name)

    client = LateAPI(uid="unused")
    assert start_fresh(client, deadline=2) is None
    assert client.reads == 0


def test_kubernetes_default_false_host_fields_may_be_omitted() -> None:
    # PodSpec's three host flags use omitempty; omission retains the false default.
    # https://github.com/kubernetes/api/blob/v0.35.0/core/v1/types.go
    class DefaultedAPI(StartingAPI):
        async def read_pod(self, namespace: str, name: str) -> dict[str, Any]:
            pod = await super().read_pod(namespace, name)
            for key in ("hostNetwork", "hostPID", "hostIPC"):
                pod["spec"].pop(key)
            # Core/v1 defaulting and standard admission additions are bounded.
            # https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/apis/core/v1/defaults.go
            pod["spec"].update(
                dnsPolicy="ClusterFirst",
                serviceAccount="default",
                serviceAccountName="default",
                schedulerName="default-scheduler",
                priority=0,
                preemptionPolicy="PreemptLowerPriority",
                tolerations=[
                    {
                        "key": key,
                        "operator": "Exists",
                        "effect": "NoExecute",
                        "tolerationSeconds": 300,
                    }
                    for key in ("node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable")
                ],
            )
            pod["spec"]["containers"][0].update(
                terminationMessagePath="/dev/termination-log", terminationMessagePolicy="File"
            )
            return pod

    pod = start_fresh(DefaultedAPI(uid="unused"))
    assert pod is not None and pod.uid == "fresh-pod"


def test_admission_cannot_add_network_capabilities_to_a_fresh_probe() -> None:
    class CapableAPI(StartingAPI):
        async def read_pod(self, namespace: str, name: str) -> dict[str, Any]:
            pod = await super().read_pod(namespace, name)
            pod["spec"]["containers"][0]["securityContext"]["capabilities"]["add"] = ["NET_ADMIN"]
            return pod

    with pytest.raises(ProbeKitRefused):
        start_fresh(CapableAPI(uid="unused"))


@pytest.mark.parametrize(
    "change", ["secret", "lifecycle", "run_as_root", "unconfined", "probe", "mount"]
)
def test_admission_cannot_add_credentials_execution_or_security_overrides(change: str) -> None:
    class InjectedAPI(StartingAPI):
        async def read_pod(self, namespace: str, name: str) -> dict[str, Any]:
            pod = await super().read_pod(namespace, name)
            container = pod["spec"]["containers"][0]
            if change == "secret":
                container["envFrom"] = [{"secretRef": {"name": "unrequested"}}]
            elif change == "lifecycle":
                container["lifecycle"] = {"postStart": {"exec": {"command": ["other"]}}}
            elif change == "run_as_root":
                container["securityContext"].update(runAsUser=0, runAsNonRoot=False)
            elif change == "unconfined":
                container["securityContext"]["seccompProfile"] = {"type": "Unconfined"}
            elif change == "probe":
                container["readinessProbe"] = {"exec": {"command": ["other"]}}
            else:
                container["volumeMounts"] = [{"name": "unrequested", "mountPath": "/input"}]
            return pod

    with pytest.raises(ProbeKitRefused):
        start_fresh(InjectedAPI(uid="unused"))
