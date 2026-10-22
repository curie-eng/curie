"""The CNI identity and the least-privilege self-check on a disposable local cluster.

@spec MANAGED-CNI-1 @spec MANAGED-CNI-5 @spec MANAGED-CNI-9. Opt-in: it runs
only with an explicit kubeconfig file, context and minikube profile for a
disposable local cluster whose API server is on loopback, with Calico. It never
reads the default kubeconfig or its current context, and it adds and deletes a
node of that profile only.

An administrator fixture installs the measuring ServiceAccount with exactly
the clause 5 manifest. Under that ServiceAccount's short-lived token only:

- the least-privilege self-check passes; a directly bound extra right and a
  group-granted extra right each refuse it, and removing them restores it;
- the identity is stable across reads and a reobservation returns the CNI
  digest within the per-call deadline;
- adding a node refuses with ``eligible_node_added``; deleting it restores the
  measured identity exactly;
- tainting an eligible node out refuses with ``eligible_node_removed``.

Local results qualify no provider's CNI.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from typing import Any

import pytest
from curie_protected_hooks.measuring_identity_rbac import (
    MeasuringScope,
    check_least_privilege,
    probe_attributes,
)
from curie_worker.managed_cni_identity import (
    CniIdentityFact,
    ManagedIdentityAPI,
    ModeKey,
    Placement,
    ProviderEntry,
    WorkloadRef,
    cni_digest,
    identity_change,
    observe_identity,
)
from curie_worker.managed_cni_kubernetes import MeasuringCredentials
from curie_worker.managed_cni_probe_kit import ProbeKitRefused

from .test_managed_cni_cluster import (
    CONTROL,
    FOREIGN,
    MEASURING,
    PROBE_A,
    PROT,
    RELEASE,
    apply,
    cluster_origin,
    kubectl,
    measuring_rbac,
    namespace,
    role,
    rule,
)

KUBECONFIG = os.environ.get("CURIE_MANAGED_CNI_KUBECONFIG", "")
CONTEXT = os.environ.get("CURIE_MANAGED_CNI_CONTEXT", "")
PROFILE = os.environ.get("CURIE_MANAGED_CNI_PROFILE", "")
EVIDENCE = os.environ.get("CURIE_MANAGED_CNI_EVIDENCE", "")
pytestmark = pytest.mark.skipif(
    not (KUBECONFIG and CONTEXT and PROFILE),
    reason="needs an explicit disposable local cluster kubeconfig, context and minikube profile",
)

CALICO_GROUPS = (
    ("crd.projectcalico.org", "networkpolicies"),
    ("crd.projectcalico.org", "globalnetworkpolicies"),
)
PROVIDER = ProviderEntry(
    provider="minikube-calico",
    cni_plugin=(WorkloadRef("DaemonSet", "kube-system", "calico-node", "k8s-app=calico-node"),),
    policy_agent=(
        WorkloadRef(
            "Deployment",
            "kube-system",
            "calico-kube-controllers",
            "k8s-app=calico-kube-controllers",
        ),
    ),
    mode_keys=(
        ModeKey("calico-node", "calico-node", env="FELIX_DEFAULTENDPOINTTOHOSTACTION"),
        ModeKey("calico-node", "calico-node", env="CALICO_NETWORKING_BACKEND"),
        ModeKey("calico-kube-controllers", "calico-kube-controllers", env="ENABLED_CONTROLLERS"),
    ),
    addon_configmaps=(("kube-system", "calico-config"),),
)
PLACEMENTS = (Placement.of({"nodeSelector": {"kubernetes.io/os": "linux"}}),)
RELEASE_BINDING = "curie-release-probe-e1"
TAINT = "curietech.ai/e3-ineligible=yes:NoSchedule"


def scope() -> MeasuringScope:
    return MeasuringScope(
        service_account=MEASURING,
        control_namespace=CONTROL,
        protected_namespace=PROT,
        probe_namespaces=(PROBE_A, FOREIGN),
        release_namespace=RELEASE,
        release_binding=RELEASE_BINDING,
        channel_rules=(("", "configmaps", ("create", "get", "list", "watch")),),
        cni_configmaps=PROVIDER.configmaps,
        provider_policies=CALICO_GROUPS,
    )


def fixture() -> list[dict[str, Any]]:
    documents = [namespace(name) for name in (PROT, CONTROL, PROBE_A, FOREIGN, RELEASE)]
    documents += measuring_rbac(CALICO_GROUPS)
    documents += role(
        "Role",
        "curie-measuring-channel",
        CONTROL,
        [rule([""], ["configmaps"], ["create", "get", "list", "watch"])],
    )
    documents += role(
        "Role",
        "curie-measuring-cni",
        "kube-system",
        [
            rule(
                [""],
                ["configmaps"],
                ["get"],
                resourceNames=[name for _ns, name in PROVIDER.configmaps],
            )
        ],
    )
    return documents


def minikube(*args: str, timeout: int = 900) -> None:
    subprocess.run(
        ["minikube", "-p", PROFILE, *args],
        env=os.environ | {"KUBECONFIG": KUBECONFIG},
        check=True,
        capture_output=True,
        timeout=timeout,
    )


def settle() -> None:
    """Wait for every node and the CNI workloads to be ready."""
    kubectl("wait", "--for=condition=Ready", "nodes", "--all", "--timeout=600s")
    for kind, name in (
        ("daemonset", "calico-node"),
        ("daemonset", "kube-proxy"),
        ("deployment", "calico-kube-controllers"),
    ):
        kubectl("-n", "kube-system", "rollout", "status", f"{kind}/{name}", "--timeout=600s")


def node_names() -> list[str]:
    nodes = json.loads(kubectl("get", "nodes", "-o", "json"))["items"]
    return sorted(item["metadata"]["name"] for item in nodes)


def test_identity_and_least_privilege_on_a_local_cluster() -> None:
    apply(fixture())
    settle()
    evidence: dict[str, Any] = {"nodes": node_names()}
    token = kubectl("create", "token", MEASURING, "-n", CONTROL, "--duration=1h").strip()
    origin, ca_file = cluster_origin()
    client = ManagedIdentityAPI(
        MeasuringCredentials(origin, ca_file, token),
        namespaces=(PROT, PROBE_A, FOREIGN, RELEASE),
        policy_namespaces=(PROBE_A, FOREIGN),
        provider=PROVIDER,
    )

    loop = asyncio.new_event_loop()

    def run(coroutine: Any) -> Any:
        return loop.run_until_complete(coroutine)

    def identity() -> Any:
        return run(observe_identity(client, PROVIDER, PLACEMENTS))

    def least() -> str | None:
        return run(check_least_privilege(client, scope(), release_allowed=True))

    try:
        # 1. Least privilege: exact manifest passes; any extra right refuses.
        evidence["least_privilege_exact"] = least()
        assert evidence["least_privilege_exact"] is None
        assert run(check_least_privilege(client, scope(), release_allowed=False)) == (
            "measuring_bindings_differ"
        )
        direct = role("Role", "e3-extra", "default", [rule([""], ["secrets"], ["get"])])
        apply(direct)
        evidence["least_privilege_direct_extra"] = least()
        assert evidence["least_privilege_direct_extra"] == "measuring_bindings_differ"
        kubectl("delete", "-n", "default", "rolebinding/e3-extra", "role/e3-extra")
        group = role("Role", "e3-group", "default", [rule([""], ["secrets"], ["get"])])
        group[1]["subjects"] = [
            {
                "kind": "Group",
                "name": f"system:serviceaccounts:{CONTROL}",
                "apiGroup": "rbac.authorization.k8s.io",
            }
        ]
        apply(group)
        # Group bindings may carry only Kubernetes' default grants.
        evidence["least_privilege_group_extra"] = least()
        assert evidence["least_privilege_group_extra"] == "measuring_bindings_differ"
        kubectl("delete", "-n", "default", "rolebinding/e3-group", "role/e3-group")
        user = role("Role", "e3-user", "default", [rule(["apps"], ["deployments"], ["create"])])
        user[1]["subjects"] = [
            {
                "kind": "User",
                "name": f"system:serviceaccount:{CONTROL}:{MEASURING}",
                "apiGroup": "rbac.authorization.k8s.io",
            }
        ]
        apply(user)
        evidence["least_privilege_user_form_extra"] = least()
        assert evidence["least_privilege_user_form_extra"] == "measuring_bindings_differ"
        kubectl("delete", "-n", "default", "rolebinding/e3-user", "role/e3-user")
        namespaces = json.loads(kubectl("get", "namespaces", "-o", "json"))["items"]
        evidence["least_privilege_reviews"] = len(
            probe_attributes(
                scope(), [item["metadata"]["name"] for item in namespaces], release=True
            )
        )
        assert least() is None

        # 2. The identity is stable and reobserves within the per-call deadline.
        campaign = identity()
        evidence["identity"] = campaign.document
        # Calico installs its CNI binary from an init container; it is in the identity.
        containers = {item["container"] for item in campaign.document["cni_plugin_images"]}
        assert "install-cni" in containers, containers
        assert campaign.document["dataplane_mode"]["kube_proxy"]["form"] == "daemonset"
        evidence["identity_digest"] = campaign.digest
        assert identity().digest == campaign.digest

        async def guard() -> str:
            return "0" * 64

        async def least_privilege() -> str | None:
            return await check_least_privilege(client, scope(), release_allowed=True)

        fact = CniIdentityFact(
            client,
            PROVIDER,
            PLACEMENTS,
            campaign=campaign,
            guard_config_digest="0" * 64,
            guard=guard,
            least_privilege=least_privilege,
        )
        started = time.monotonic()
        assert run(fact.observe()) == cni_digest(campaign.digest)
        evidence["reobservation_ms"] = round((time.monotonic() - started) * 1000)

        # 3. A new eligible node invalidates; deleting it restores the identity.
        before = node_names()
        minikube("node", "add")
        settle()
        added = sorted(set(node_names()) - set(before))
        assert len(added) == 1
        evidence["added_node"] = added[0]
        evidence["after_add"] = identity_change(campaign, identity())
        assert evidence["after_add"] == "eligible_node_added"
        with pytest.raises(ProbeKitRefused):
            run(fact.observe())
        evidence["after_add_fact_refusal"] = fact.refusal
        assert fact.refusal == "eligible_node_added"
        minikube("node", "delete", added[0])
        settle()
        assert node_names() == before
        evidence["after_delete"] = identity_change(campaign, identity())
        assert evidence["after_delete"] is None
        assert run(fact.observe()) == cni_digest(campaign.digest)

        # 4. Tainting an eligible node out invalidates; untainting restores it.
        target = before[-1]
        kubectl("taint", "nodes", target, TAINT)
        try:
            evidence["after_taint"] = identity_change(campaign, identity())
            assert evidence["after_taint"] == "eligible_node_removed"
        finally:
            kubectl("taint", "nodes", target, TAINT + "-")
        assert identity_change(campaign, identity()) is None
    finally:
        run(client.close())
        loop.close()
        if EVIDENCE:
            with open(EVIDENCE, "w") as handle:
                json.dump(evidence, handle, indent=1, sort_keys=True)
