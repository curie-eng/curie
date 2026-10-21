"""The local Cilium network and peer facts on a disposable multi-node minikube profile, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. Opt-in: it runs only with an explicit
kubeconfig file, context and minikube profile of a disposable local cluster
running Cilium, whose API server is on loopback. It never reads the default
kubeconfig or its current context.

The administrator fixture of the probe kit's cluster test stands in for the
installation bootstrap: namespaces, role-label NetworkPolicies, stand-in broker
and mediator, a declared egress peer, the stand-in guard and the measuring
ServiceAccount. This test adds the Cilium reads to the measuring identity and
an owned off-cluster fixture container on the node network as the local
``cloud_network`` destination. It then runs the producer and calls the
network, hostile-peer and retained-peer facts. It shows that a policy or
engine configuration change refuses the next network call and that a
non-enforcing configuration refuses before any probe pod.
Local results qualify no other installation and no managed CNI.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from curie_worker.commissioning_local_cilium import (
    CILIUM_AGENT_OBJECTS,
    CILIUM_POLICY_COLLECTIONS,
    LocalCiliumAPI,
)
from curie_worker.commissioning_local_network import (
    LocalCiliumNetworkProducer,
    local_required_entries,
)
from curie_worker.managed_cni_cases import (
    BUILT_IN_GROUPS,
    Destination,
    KitResult,
    ProbeLayout,
    Subject,
    Target,
)
from curie_worker.managed_cni_kubernetes import MeasuringCredentials
from curie_worker.managed_cni_probe_kit import Measurement, ProbeKitRefused, served_families
from curie_worker.managed_cni_probe_programs import PROBE_IMAGE

sys.path.insert(0, str(Path(__file__).parent))
import test_managed_cni_cluster as fixture  # noqa: E402

KUBECONFIG = os.environ.get("CURIE_LOCAL_CILIUM_KUBECONFIG", "")
CONTEXT = os.environ.get("CURIE_LOCAL_CILIUM_CONTEXT", "")
PROFILE = os.environ.get("CURIE_LOCAL_CILIUM_PROFILE", "")
EVIDENCE = os.environ.get("CURIE_LOCAL_CILIUM_EVIDENCE", "")
pytestmark = pytest.mark.skipif(
    not (KUBECONFIG and CONTEXT and PROFILE),
    reason="needs an explicit disposable local Cilium cluster kubeconfig, context and profile",
)

PROT, CONTROL, PROBE_A, FOREIGN, RELEASE = (
    fixture.PROT,
    fixture.CONTROL,
    fixture.PROBE_A,
    fixture.FOREIGN,
    fixture.RELEASE,
)
FIXTURE_PORT = 18081
FIXTURE_LABEL = "curietech.ai/local-cilium-fixture"


def kubectl(*args: str, stdin: str | None = None) -> str:
    return fixture.kubectl(*args, stdin=stdin)


def docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True, timeout=300
    ).stdout


def cilium_reads() -> list[dict[str, Any]]:
    """ADR 0238 clause 5 reads of the CNI and dataplane workloads, beyond the kit's."""
    configs = ["cilium-config", "kube-proxy"]
    return fixture.role(
        "Role",
        "curie-measuring-cilium",
        "kube-system",
        [
            fixture.rule([""], ["configmaps"], ["get"], resourceNames=configs),
            fixture.rule(["apps"], ["replicasets"], ["list"]),
        ],
    )


def off_cluster_fixture() -> tuple[str, str]:
    """An owned listener container on the minikube node network, outside the cluster."""
    networks = json.loads(
        docker("inspect", PROFILE, "--format", "{{json .NetworkSettings.Networks}}")
    )
    assert len(networks) == 1, networks
    network = next(iter(networks))
    name = "curie-d8-fixture-" + secrets.token_hex(4)
    docker(
        "run",
        "--detach",
        "--name",
        name,
        "--label",
        f"{FIXTURE_LABEL}={PROFILE}",
        "--network",
        network,
        PROBE_IMAGE,
        "python3",
        "-m",
        "http.server",
        str(FIXTURE_PORT),
    )
    address = json.loads(docker("inspect", name, "--format", "{{json .NetworkSettings.Networks}}"))[
        network
    ]["IPAddress"]
    return name, address


def install() -> tuple[list[str], dict[str, Any]]:
    nodes = fixture.ready_nodes()
    assert len(nodes) >= 2, nodes
    fixture_ns = {PROT, CONTROL, PROBE_A, FOREIGN, RELEASE}
    system_set = frozenset(
        s
        for s in fixture.binding_subjects()
        if s.namespace not in fixture_ns and s not in BUILT_IN_GROUPS
    )
    fixture.apply(
        [
            fixture.namespace(PROT),
            fixture.namespace(CONTROL),
            fixture.namespace(PROBE_A, "privileged"),
            fixture.namespace(FOREIGN, "privileged"),
            fixture.namespace(RELEASE),
        ]
    )
    fixture.apply(fixture.guard())
    provider = [("cilium.io", c.rsplit("/", 1)[1]) for c in CILIUM_POLICY_COLLECTIONS]
    fixture.apply(fixture.measuring_rbac(provider) + cilium_reads())
    # An ordinary release workload identity with ordinary rights in its namespace.
    rules = [fixture.rule([""], ["pods", "configmaps"], ["get", "list", "create", "delete"])]
    fixture.apply(
        [
            {
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "app", "namespace": RELEASE},
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {"name": "app", "namespace": RELEASE},
                "rules": rules,
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": "app", "namespace": RELEASE},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "app"},
                "subjects": [{"kind": "ServiceAccount", "name": "app", "namespace": RELEASE}],
            },
        ]
    )
    role = fixture.ROLE
    fixture.apply(
        [
            fixture.policy("default-deny-ingress", "", {"policyTypes": ["Ingress"]}),
            fixture.policy(
                "broker-ingress", "broker", fixture.ingress_from("broker-client", 16379)
            ),
            fixture.policy(
                "mediator-ingress", "mediator", fixture.ingress_from("mediator-client", 18443)
            ),
            fixture.policy(
                "declared-peer-ingress", "declared-peer", fixture.ingress_from("runner", 18080)
            ),
            fixture.policy(
                "runner-egress",
                "runner",
                {
                    "policyTypes": ["Egress"],
                    "egress": [
                        {
                            "to": [{"podSelector": {"matchLabels": {role: "declared-peer"}}}],
                            "ports": [{"protocol": "TCP", "port": 18080}],
                        }
                    ],
                },
            ),
        ]
    )
    stand_ins = (
        fixture.stand_in("broker-real", nodes[-1], {role: "broker", "app": "broker-real"}, 16379)
        + fixture.stand_in(
            "mediator-real", nodes[-1], {role: "mediator", "app": "mediator-real"}, 18443
        )
        + fixture.stand_in(
            "declared-peer", nodes[-1], {role: "declared-peer", "app": "declared-peer"}, 18080
        )
    )
    for document in stand_ins:
        if document["kind"] == "Pod":
            document["metadata"]["labels"].pop("curietech.ai/managed-cni-probe-template")
    names = ("broker-real", "mediator-real", "declared-peer")
    kubectl("delete", "pod", "-n", PROT, *names, "--ignore-not-found", "--wait=true")
    fixture.apply(stand_ins)
    kubectl(
        "wait", "-n", PROT, "--for=condition=Ready", *("pod/" + n for n in names), "--timeout=240s"
    )
    return nodes, {"system_set": system_set}


def service_ip(name: str, ns: str = PROT) -> str:
    return str(json.loads(kubectl("get", "svc", "-n", ns, name, "-o", "json"))["spec"]["clusterIP"])


def layout(nodes: list[str], system_set: frozenset[Subject], cloud: Destination) -> ProbeLayout:
    role = fixture.ROLE
    peer_ip = json.loads(kubectl("get", "pod", "-n", PROT, "declared-peer", "-o", "json"))[
        "status"
    ]["podIP"]
    slices = json.loads(
        kubectl(
            "get",
            "endpointslices",
            "-n",
            "default",
            "-l",
            "kubernetes.io/service-name=kubernetes",
            "-o",
            "json",
        )
    )["items"]
    api = [Destination(service_ip("kubernetes", "default"), 443, "tcp")]
    for item in slices:
        port = item["ports"][0]["port"]
        for endpoint in item["endpoints"]:
            api += [Destination(a, port, "tcp") for a in endpoint["addresses"]]
    node_ips = [
        a["address"]
        for item in json.loads(kubectl("get", "nodes", "-o", "json"))["items"]
        for a in item["status"]["addresses"]
        if a["type"] == "InternalIP"
    ]
    targets = (
        Target(
            "broker",
            {role: "broker"},
            {role: "broker-client"},
            16379,
            ("tcp", "udp"),
            16380,
            (service_ip("broker-real"),),
        ),
        Target(
            "mediator",
            {role: "mediator"},
            {role: "mediator-client"},
            18443,
            ("tcp", "udp"),
            18444,
            (service_ip("mediator-real"),),
        ),
    )
    return ProbeLayout(
        campaign="d8",
        protected_namespace=PROT,
        probe_namespace=PROBE_A,
        foreign_namespace=FOREIGN,
        release_namespace=RELEASE,
        eligible_nodes=tuple(nodes),
        families=served_families([peer_ip], [], node_ips),
        targets=targets,
        runner_labels={role: "runner"},
        declared_peers=(Destination(peer_ip, 18080, "tcp"),),
        supplied_egress={"kubernetes_api": tuple(api), "cloud_network": (cloud,)},
        guard_names=(fixture.GUARD,),
        system_set=system_set,
        excluded=frozenset({Subject("ServiceAccount", fixture.MEASURING, CONTROL)}),
        provider_policy_collections=CILIUM_POLICY_COLLECTIONS,
        policy_agent_objects=CILIUM_AGENT_OBJECTS,
    )


def kube_proxy_present() -> bool:
    found = kubectl("get", "daemonset", "-n", "kube-system", "kube-proxy", "--ignore-not-found")
    return bool(found.strip())


def kube_proxy_mode(identity: Any) -> str | None:
    proxy = identity.document["kube_proxy"] if identity else None
    if proxy is None:
        return None
    lines = proxy["config"]["data"].get("config.conf", "").splitlines()
    return next((line for line in lines if line.startswith("mode:")), "mode: <unset>")


class NoKit:
    """Fails the test if the producer starts a behavioral case."""

    last: KitResult | None = None

    async def measure(self) -> Measurement:
        raise AssertionError("the kit ran against a non-enforcing configuration")


def patch_config(data: dict[str, Any], name: str = "cilium-config") -> None:
    kubectl(
        "patch",
        "configmap",
        "-n",
        "kube-system",
        name,
        "--type",
        "merge",
        "-p",
        json.dumps({"data": data}),
    )


def remove_config_key(key: str, name: str = "cilium-config") -> None:
    kubectl(
        "patch",
        "configmap",
        "-n",
        "kube-system",
        name,
        "--type",
        "json",
        "-p",
        json.dumps([{"op": "remove", "path": "/data/" + key}]),
    )


def test_local_cilium_network_producer_on_a_local_cluster() -> None:
    fixture.KUBECONFIG, fixture.CONTEXT = KUBECONFIG, CONTEXT
    nodes, extra = install()
    container, address = off_cluster_fixture()
    origin, ca_file = fixture.cluster_origin()
    token = kubectl("create", "token", fixture.MEASURING, "-n", CONTROL, "--duration=1h").strip()
    client = LocalCiliumAPI(
        MeasuringCredentials(origin, ca_file, token),
        namespaces=(PROT, PROBE_A, FOREIGN, RELEASE),
        # Clause 5: NetworkPolicy writes (case 5 policy_changed) only in probe namespaces.
        policy_namespaces=(PROBE_A, FOREIGN),
    )
    probe_layout = layout(nodes, extra["system_set"], Destination(address, FIXTURE_PORT, "tcp"))
    producer = LocalCiliumNetworkProducer(client, probe_layout, profile=PROFILE)
    fact = producer.fact("network")
    calls: list[dict[str, Any]] = []

    async def call(label: str) -> str | None:
        started = time.monotonic()
        try:
            digest: str | None = await fact.observe()
        except ProbeKitRefused:
            digest = None
        calls.append(
            {"call": label, "digest": digest, "ms": round((time.monotonic() - started) * 1000)}
        )
        return digest

    async def run() -> dict[str, Any]:
        try:
            measurement = await producer.measure()
            peers = {}
            for name in ("hostile_peer", "retained_peer"):
                try:
                    peers[name] = await producer.fact(name).observe()
                except ProbeKitRefused:
                    peers[name] = None
            first = await call("first")
            second = await call("second")
            kubectl(
                "apply",
                "-f",
                "-",
                stdin=json.dumps(
                    fixture.policy("d8-added", "nobody", {"policyTypes": ["Ingress"]})
                ),
            )
            added = await call("policy_added")
            kubectl("delete", "networkpolicy", "-n", PROT, "d8-added", "--wait=true")
            restored = await call("policy_removed")
            patch_config({"curie-d8-identity-probe": "1"})
            changed = await call("config_changed")
            remove_config_key("curie-d8-identity-probe")
            back = await call("config_restored")
            proxy = kube_proxy_present()
            if proxy:
                patch_config({"curie-d8-identity-probe": "1"}, "kube-proxy")
            proxy_changed = await call("kube_proxy_config_changed") if proxy else None
            if proxy:
                remove_config_key("curie-d8-identity-probe", "kube-proxy")
            proxy_back = await call("kube_proxy_config_restored") if proxy else back
            patch_config({"policy-audit-mode": "true"})
            try:
                audit = await LocalCiliumNetworkProducer(
                    client, probe_layout, profile=PROFILE, kit=NoKit()
                ).measure()
                audit_call = await call("audit_mode")
            finally:
                remove_config_key("policy-audit-mode")
            return {
                "measurement": measurement,
                "peers": peers,
                "digests": (first, second, added, restored, changed, back, audit_call),
                "proxy": (proxy_changed, proxy_back),
                "audit": audit,
            }
        finally:
            await client.close()

    try:
        outcome = asyncio.run(run())
    finally:
        docker("rm", "--force", container)
    measurement: Measurement = outcome["measurement"]
    result = producer.last
    assert result is not None and result.kit is not None
    summary = sorted((list(item.key), item.outcome, item.reason) for item in measurement.entries)
    if EVIDENCE:
        with open(EVIDENCE, "w") as handle:
            json.dump(
                {
                    "profile": PROFILE,
                    "nodes": nodes,
                    "families": list(probe_layout.families),
                    "identity": result.identity.document if result.identity else None,
                    "refusal": measurement.refusal,
                    "entries": summary,
                    "fact_calls": calls,
                    "peer_facts": outcome["peers"],
                    "audit_mode_refusal": outcome["audit"].refusal,
                    "kube_proxy_mode": kube_proxy_mode(result.identity),
                    "kit_evidence": list(result.kit.evidence),
                    "leftovers": list(result.kit.leftovers),
                },
                handle,
                indent=1,
                sort_keys=True,
            )
    assert result.kit.leftovers == ()
    assert result.bracket is None, summary
    assert {item.key for item in measurement.entries} == local_required_entries(probe_layout)
    # No delivery on an enforcing local Cilium: a fail would be a real finding.
    assert not [item for item in measurement.entries if item.outcome == "fail"], summary
    # Every local entry outside cases 3, 4 and 5 passes. A timing case this local
    # cluster cannot provoke is an honest unavailable, never a pass.
    assert all(item.outcome == "pass" for item in measurement.entries if item.case in (1, 2, 6, 8))
    not_passed = {item.reason for item in measurement.entries if item.outcome != "pass"}
    assert not_passed <= {
        "reuse_not_provoked",
        "transport_unavailable",
        "sequence_state_unavailable",
        "address_reassigned",
        "phase_timeout",
        "control_failed",
    }, summary
    # Case 5 workload_replaced cannot pass from user space, so the producer refuses.
    assert measurement.refusal is not None
    # Each peer fact is a digest exactly when every entry of its own channel passed.
    for name, cases in (("hostile_peer", (2, 3, 4)), ("retained_peer", (5,))):
        clean = all(item.outcome == "pass" for item in measurement.entries if item.case in cases)
        assert (outcome["peers"][name] is not None) == clean, (name, summary)
    assert outcome["peers"]["retained_peer"] is None
    first, second, added, restored, changed, back, audit_call = outcome["digests"]
    assert first is not None and first == second == restored == back
    assert added is None and changed is None and audit_call is None
    assert outcome["proxy"] == (None, first)
    assert (outcome["audit"].entries, outcome["audit"].refusal) == ((), "cni_unavailable")
    identity = result.identity
    assert identity is not None
    assert identity.document["version"]["gitVersion"].startswith("v1.")
    assert identity.document["operator_set"]["uid"] in {
        pod["owner"] for pod in identity.document["pods"]["operator"]
    }
