"""Cases 1, 2, 6 and 8 on a disposable local multi-node cluster, ADR 0238.

@spec MANAGED-CNI-2 @spec MANAGED-CNI-4 @spec MANAGED-CNI-5. Opt-in: it runs
only with an explicit kubeconfig file and context for a disposable local
cluster whose API server is on loopback, for example a named minikube profile
started with ``KUBECONFIG`` pointing at a private file. It never reads the
default kubeconfig or its current context.

An administrator fixture stands in for the installation bootstrap of a later
slice: namespaces, role-label NetworkPolicies, stand-in broker and mediator
listeners behind Services, a declared egress peer, a ValidatingAdmissionPolicy
standing in for the guard, and the measuring ServiceAccount bound to exactly
the clause 5 rules this slice uses. The kit then runs under that
ServiceAccount's short-lived token only.

``CURIE_MANAGED_CNI_EXPECT=enforcing``: case 1 passes in every placement and no
entry fails; the provider destinations a local cluster lacks are
``unavailable``. ``CURIE_MANAGED_CNI_EXPECT=absent``: case 1 fails with
``enforcement_absent`` and no other case runs. Local results qualify no
provider's CNI.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import tempfile
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

import pytest
import yaml
from curie_worker.managed_cni_cases import (
    BUILT_IN_GROUPS,
    Destination,
    ProbeKit,
    ProbeLayout,
    Subject,
    Target,
    required_entries,
)
from curie_worker.managed_cni_kubernetes import MeasuringCredentials, MeasuringIdentityAPI
from curie_worker.managed_cni_probe_kit import served_families
from curie_worker.managed_cni_probe_programs import probe_pod

KUBECONFIG = os.environ.get("CURIE_MANAGED_CNI_KUBECONFIG", "")
CONTEXT = os.environ.get("CURIE_MANAGED_CNI_CONTEXT", "")
EXPECT = os.environ.get("CURIE_MANAGED_CNI_EXPECT", "")
EVIDENCE = os.environ.get("CURIE_MANAGED_CNI_EVIDENCE", "")
pytestmark = pytest.mark.skipif(
    not (KUBECONFIG and CONTEXT and EXPECT in ("enforcing", "absent")),
    reason="needs an explicit disposable local cluster kubeconfig, context and expectation",
)

ROLE = "curietech.ai/policy-role"
PROT, CONTROL, PROBE_A, FOREIGN, RELEASE = "prot", "control", "probe-a", "probe-foreign", "release"
GUARD = "curie-fixture-guard.curietech.ai"
MEASURING = "measuring"
MEASURING_USER = f"system:serviceaccount:{CONTROL}:{MEASURING}"
CALICO_POLICIES = (
    "/apis/crd.projectcalico.org/v1/networkpolicies",
    "/apis/crd.projectcalico.org/v1/globalnetworkpolicies",
)


def kubectl(*args: str, stdin: str | None = None) -> str:
    return subprocess.run(
        ["kubectl", "--kubeconfig", KUBECONFIG, "--context", CONTEXT, *args],
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
        timeout=300,
    ).stdout


def apply(documents: Sequence[dict[str, Any]]) -> None:
    kubectl(
        "apply",
        "-f",
        "-",
        stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": list(documents)}),
    )


def cluster_origin() -> tuple[str, str]:
    config = yaml.safe_load(kubectl("config", "view", "--raw", "--minify", "-o", "yaml"))
    cluster = config["clusters"][0]["cluster"]
    origin = cluster["server"]
    # Disposable local clusters only: the API server must be on loopback.
    assert urlsplit(origin).hostname in ("127.0.0.1", "localhost", "::1"), origin
    if "certificate-authority" in cluster:
        return origin, cluster["certificate-authority"]
    handle = tempfile.NamedTemporaryFile("wb", suffix=".crt", delete=False)
    handle.write(base64.b64decode(cluster["certificate-authority-data"]))
    handle.close()
    return origin, handle.name


def namespace(name: str, level: str = "baseline") -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": {
                "curietech.ai/managed-cni-fixture": "e1",
                "pod-security.kubernetes.io/enforce": level,
            },
        },
    }


def policy(name: str, selector: str, spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name, "namespace": PROT},
        "spec": {"podSelector": {"matchLabels": {ROLE: selector}} if selector else {}, **spec},
    }


def ingress_from(client: str, port: int) -> dict[str, Any]:
    return {
        "policyTypes": ["Ingress"],
        "ingress": [
            {
                "from": [{"podSelector": {"matchLabels": {ROLE: client}}}],
                "ports": [{"protocol": "TCP", "port": port}, {"protocol": "UDP", "port": port}],
            }
        ],
    }


def rule(groups: list[str], resources: list[str], verbs: list[str], **extra: Any) -> dict[str, Any]:
    return {"apiGroups": groups, "resources": resources, "verbs": verbs, **extra}


def role(kind: str, name: str, ns: str | None, rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    meta = {"name": name} | ({"namespace": ns} if ns else {})
    binding_kind = "RoleBinding" if kind == "Role" else "ClusterRoleBinding"
    return [
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": kind,
            "metadata": meta,
            "rules": rules,
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": binding_kind,
            "metadata": meta,
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": kind, "name": name},
            "subjects": [{"kind": "ServiceAccount", "name": MEASURING, "namespace": CONTROL}],
        },
    ]


def measuring_rbac(provider_groups: Sequence[tuple[str, str]]) -> list[dict[str, Any]]:
    """Clause 5, the rules this slice uses. The channel Role belongs to a later slice."""
    probe = [
        rule([""], ["pods"], ["create", "get", "list", "watch", "patch", "delete"]),
        rule([""], ["pods/log", "pods/status"], ["get"]),
        rule(
            ["networking.k8s.io"],
            ["networkpolicies"],
            ["create", "get", "list", "watch", "update", "patch", "delete"],
        ),
    ]
    documents: list[dict[str, Any]] = [
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": MEASURING, "namespace": CONTROL},
        },
    ]
    documents += role("Role", "curie-measuring", PROBE_A, probe)
    documents += role("Role", "curie-measuring", FOREIGN, probe)
    documents += role(
        "Role",
        "curie-measuring",
        PROT,
        [
            rule([""], ["pods"], ["create", "get", "list", "watch", "delete"]),
            rule([""], ["pods/log", "pods/status"], ["get"]),
        ],
    )
    documents += role(
        "Role",
        "curie-release-probe-e1",
        RELEASE,
        [
            rule([""], ["pods"], ["create", "get", "list", "watch", "delete"]),
            rule([""], ["pods/log"], ["get"]),
            rule(
                ["rbac.authorization.k8s.io"],
                ["rolebindings"],
                ["delete"],
                resourceNames=["curie-release-probe-e1"],
            ),
        ],
    )
    reads = [
        rule([""], ["nodes", "namespaces"], ["get", "list", "watch"]),
        rule([""], ["pods"], ["get", "list", "watch"]),
        rule(["apps"], ["daemonsets", "deployments"], ["get", "list", "watch"]),
        rule(["networking.k8s.io"], ["networkpolicies"], ["get", "list", "watch"]),
        rule(
            ["admissionregistration.k8s.io"],
            [
                "validatingwebhookconfigurations",
                "mutatingwebhookconfigurations",
                "validatingadmissionpolicies",
                "validatingadmissionpolicybindings",
            ],
            ["get", "list", "watch"],
        ),
        rule(
            ["rbac.authorization.k8s.io"],
            ["roles", "rolebindings", "clusterroles", "clusterrolebindings"],
            ["get", "list", "watch"],
        ),
        {"nonResourceURLs": ["/version"], "verbs": ["get"]},
    ]
    reads += [
        rule([group], [resource], ["get", "list", "watch"]) for group, resource in provider_groups
    ]
    documents += role("ClusterRole", "curie-measuring-reads", None, reads)
    documents += role(
        "ClusterRole",
        "curie-measuring-reviews",
        None,
        [
            rule(["authorization.k8s.io"], ["subjectaccessreviews"], ["create"]),
        ],
    )
    return documents


def guard() -> list[dict[str, Any]]:
    """Stand-in guard: the measuring identity may create pods only from probe templates."""
    name = GUARD
    return [
        {
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicy",
            "metadata": {"name": name},
            "spec": {
                "failurePolicy": "Fail",
                "matchConstraints": {
                    "resourceRules": [
                        {
                            "apiGroups": [""],
                            "apiVersions": ["v1"],
                            "operations": ["CREATE"],
                            "resources": ["pods"],
                        }
                    ]
                },
                "validations": [
                    {
                        "expression": (
                            f"request.userInfo.username != '{MEASURING_USER}'"
                            " || (has(object.metadata.labels) && "
                            "'curietech.ai/managed-cni-probe-template' in object.metadata.labels)"
                        ),
                        "message": "only probe templates",
                    },
                    # The eligible-node fence (clause 6): even a templated pod from the
                    # measuring identity may not request node-capable rights, so case 7
                    # admission_node_capability is non-vacuous.
                    {
                        "expression": (
                            f"request.userInfo.username != '{MEASURING_USER}'"
                            " || (object.spec.hostNetwork != true"
                            " && object.spec.hostPID != true && object.spec.hostIPC != true)"
                        ),
                        "message": "no host namespaces",
                    },
                    {
                        "expression": (
                            f"request.userInfo.username != '{MEASURING_USER}'"
                            " || object.spec.containers.all(c, !has(c.securityContext)"
                            " || c.securityContext.privileged != true)"
                        ),
                        "message": "no privileged container",
                    },
                    {
                        "expression": (
                            f"request.userInfo.username != '{MEASURING_USER}'"
                            " || !has(object.spec.volumes)"
                            " || object.spec.volumes.all(v, !has(v.hostPath))"
                        ),
                        "message": "no host path",
                    },
                    {
                        "expression": (
                            f"request.userInfo.username != '{MEASURING_USER}'"
                            " || !has(object.spec.tolerations)"
                            " || object.spec.tolerations.all(t, !has(t.key)"
                            " || t.key != 'curietech.ai/eligible-node')"
                        ),
                        "message": "no eligible-node toleration",
                    },
                ],
            },
        },
        {
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingAdmissionPolicyBinding",
            "metadata": {"name": name},
            "spec": {
                "policyName": name,
                "validationActions": ["Deny"],
                "matchResources": {
                    "namespaceSelector": {"matchLabels": {"curietech.ai/managed-cni-fixture": "e1"}}
                },
            },
        },
    ]


def stand_in(name: str, node: str, labels: dict[str, str], port: int) -> list[dict[str, Any]]:
    pod = probe_pod(
        template="twin",
        name=name,
        namespace=PROT,
        node=node,
        campaign="fixture",
        role_labels=labels,
        argument={"tcp": [port], "udp": [port]},
    )
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": PROT},
        "spec": {
            "selector": {"app": name},
            "ports": [
                {"name": "tcp", "port": port, "protocol": "TCP"},
                {"name": "udp", "port": port, "protocol": "UDP"},
            ],
        },
    }
    return [pod, service]


def ready_nodes() -> list[str]:
    nodes = json.loads(kubectl("get", "nodes", "-o", "json"))["items"]
    return sorted(
        item["metadata"]["name"]
        for item in nodes
        if any(c["type"] == "Ready" and c["status"] == "True" for c in item["status"]["conditions"])
    )


def binding_subjects() -> set[Subject]:
    found = set()
    for kind in ("rolebindings", "clusterrolebindings"):
        for item in json.loads(kubectl("get", kind, "-A", "-o", "json"))["items"]:
            ns = item["metadata"].get("namespace", "")
            for subject in item.get("subjects") or []:
                namespace = (
                    subject.get("namespace", ns) if subject["kind"] == "ServiceAccount" else ""
                )
                found.add(Subject(subject["kind"], subject["name"], namespace))
    return found


def test_probe_kit_on_a_local_cluster() -> None:
    nodes = ready_nodes()
    calico = bool(kubectl("api-resources", "--api-group=crd.projectcalico.org", "-o", "name"))
    provider_groups = [("crd.projectcalico.org", c.rsplit("/", 1)[1]) for c in CALICO_POLICIES]
    # The administrator declares every pre-existing bound subject outside the
    # fixture namespaces as the system set (a fresh disposable cluster).
    fixture_ns = {PROT, CONTROL, PROBE_A, FOREIGN, RELEASE}
    system_set = frozenset(
        s for s in binding_subjects() if s.namespace not in fixture_ns and s not in BUILT_IN_GROUPS
    )
    apply(
        [
            namespace(PROT),
            namespace(CONTROL),
            namespace(PROBE_A, "privileged"),
            namespace(FOREIGN, "privileged"),
            namespace(RELEASE),
        ]
    )
    apply(guard())
    apply(measuring_rbac(provider_groups if calico else []))
    # An ordinary release workload identity with ordinary rights in its namespace.
    apply(
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
                "rules": [rule([""], ["pods", "configmaps"], ["get", "list", "create", "delete"])],
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
    apply(
        [
            policy("default-deny-ingress", "", {"policyTypes": ["Ingress"]}),
            policy("broker-ingress", "broker", ingress_from("broker-client", 16379)),
            policy("mediator-ingress", "mediator", ingress_from("mediator-client", 18443)),
            policy("declared-peer-ingress", "declared-peer", ingress_from("runner", 18080)),
            policy(
                "runner-egress",
                "runner",
                {
                    "policyTypes": ["Egress"],
                    "egress": [
                        {
                            "to": [{"podSelector": {"matchLabels": {ROLE: "declared-peer"}}}],
                            "ports": [{"protocol": "TCP", "port": 18080}],
                        }
                    ],
                },
            ),
        ]
    )
    stand_ins = (
        stand_in("broker-real", nodes[-1], {ROLE: "broker", "app": "broker-real"}, 16379)
        + stand_in("mediator-real", nodes[-1], {ROLE: "mediator", "app": "mediator-real"}, 18443)
        + stand_in(
            "declared-peer", nodes[-1], {ROLE: "declared-peer", "app": "declared-peer"}, 18080
        )
    )
    for document in stand_ins:
        if document["kind"] == "Pod":
            document["metadata"]["labels"].pop("curietech.ai/managed-cni-probe-template")
    kubectl(
        "delete",
        "pod",
        "-n",
        PROT,
        "broker-real",
        "mediator-real",
        "declared-peer",
        "--ignore-not-found",
        "--wait=true",
    )
    apply(stand_ins)
    kubectl(
        "wait",
        "-n",
        PROT,
        "--for=condition=Ready",
        "pod/broker-real",
        "pod/mediator-real",
        "pod/declared-peer",
        "--timeout=240s",
    )

    def service_ip(name: str, ns: str = PROT) -> str:
        return str(
            json.loads(kubectl("get", "svc", "-n", ns, name, "-o", "json"))["spec"]["clusterIP"]
        )

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
    api_destinations = [Destination(service_ip("kubernetes", "default"), 443, "tcp")]
    for item in slices:
        port = item["ports"][0]["port"]
        for endpoint in item["endpoints"]:
            api_destinations += [Destination(a, port, "tcp") for a in endpoint["addresses"]]
    node_ips = [
        a["address"]
        for item in json.loads(kubectl("get", "nodes", "-o", "json"))["items"]
        for a in item["status"]["addresses"]
        if a["type"] == "InternalIP"
    ]
    families = served_families([peer_ip], [], node_ips)
    layout = ProbeLayout(
        campaign="e1",
        protected_namespace=PROT,
        probe_namespace=PROBE_A,
        foreign_namespace=FOREIGN,
        release_namespace=RELEASE,
        eligible_nodes=tuple(nodes),
        families=families,
        targets=(
            Target(
                "broker",
                {ROLE: "broker"},
                {ROLE: "broker-client"},
                16379,
                ("tcp", "udp"),
                16380,
                (service_ip("broker-real"),),
            ),
            Target(
                "mediator",
                {ROLE: "mediator"},
                {ROLE: "mediator-client"},
                18443,
                ("tcp", "udp"),
                18444,
                (service_ip("mediator-real"),),
            ),
        ),
        runner_labels={ROLE: "runner"},
        declared_peers=(Destination(peer_ip, 18080, "tcp"),),
        # A local cluster has no provider metadata service and no provider subnets.
        supplied_egress={"kubernetes_api": tuple(api_destinations)},
        guard_names=(GUARD,),
        system_set=system_set,
        excluded=frozenset({Subject("ServiceAccount", MEASURING, CONTROL)}),
        provider_policy_collections=CALICO_POLICIES if calico else (),
        policy_agent_objects=(),
    )
    token = kubectl("create", "token", MEASURING, "-n", CONTROL, "--duration=1h").strip()
    origin, ca_file = cluster_origin()
    client = MeasuringIdentityAPI(
        MeasuringCredentials(origin, ca_file, token),
        namespaces=(PROT, PROBE_A, FOREIGN, RELEASE),
        # Clause 5: NetworkPolicy writes (case 5 policy_changed) only in probe namespaces.
        policy_namespaces=(PROBE_A, FOREIGN),
    )
    kit = ProbeKit(client, layout)

    async def run() -> None:
        try:
            await kit.measure()
        finally:
            await client.close()

    asyncio.run(run())
    result = kit.last
    assert result is not None
    measurement = result.measurement
    summary = sorted((list(item.key), item.outcome, item.reason) for item in measurement.entries)
    if EVIDENCE:
        with open(EVIDENCE, "w") as handle:
            json.dump(
                {
                    "nodes": nodes,
                    "families": list(families),
                    "calico": calico,
                    "refusal": measurement.refusal,
                    "entries": summary,
                    "evidence": list(result.evidence),
                },
                handle,
                indent=1,
                sort_keys=True,
            )
    assert result.leftovers == ()
    case_one = [item for item in measurement.entries if item.case == 1]
    assert len(case_one) == 4 * len(families)
    if EXPECT == "absent":
        assert measurement.refusal == "enforcement_absent", summary
        assert {(i.outcome, i.reason) for i in case_one} == {("fail", "enforcement_absent")}
        assert {item.case for item in measurement.entries} == {1}
        return
    assert {i.outcome for i in case_one} == {"pass"}, summary
    # The probe kit produced exactly the required entry set, once each.
    assert set(required_entries(layout)) == {item.key for item in measurement.entries}
    not_passed = {item.key: item.reason for item in measurement.entries if item.outcome != "pass"}
    provider = {
        (6, sub_case, family, "none", "runner_policy")
        for sub_case in ("instance_metadata", "cloud_network")
        for family in families
    }
    # A local cluster has no provider destinations. A destination this host's
    # network drops for every pod proves no denial, so it is unavailable.
    assert {key for key, reason in not_passed.items() if reason == "destination_unknown"} == (
        provider
    ), summary
    # The enforcement cases (1-6) must never report a delivery on an enforcing CNI:
    # a leak there would be a real finding, not a local limitation. Only
    # unavailable (a case this local cluster could not provoke) is allowed.
    leaks = [
        item for item in measurement.entries if item.case in (1, 2, 3, 4, 5, 6)
        and item.outcome == "fail"
    ]
    assert leaks == [], summary
    # Every non-pass is an honest unavailable with a known reason. Timing cases a
    # local cluster cannot provoke (address reuse, a recently terminated address,
    # node-local kubelet reach) are unavailable, never a pass.
    allowed_unavailable = {
        "destination_unknown", "destination_unreached", "reuse_not_provoked",
        "transport_unavailable", "node_probe_unavailable", "phase_timeout",
        "single_eligible_node", "no_family_address", "control_failed", "review_unavailable",
        "address_reassigned",
    }
    assert all(item.outcome == "unavailable" for item in measurement.entries
               if item.outcome != "pass"), summary
    assert set(not_passed.values()) <= allowed_unavailable, summary
    # Case 7 may legitimately refuse (ordinary identities with node-capable rights);
    # that is a node-authority finding, recorded, not a CNI leak.
    assert measurement.refusal is None or measurement.refusal in (
        allowed_unavailable | {"separate_authority_domain_required", "node_capable_pod"}
    ), summary
