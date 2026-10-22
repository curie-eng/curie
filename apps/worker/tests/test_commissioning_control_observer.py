"""The ``control_profile`` fact. @spec PROTECTED-COMMISSIONING-5, ADR 0236.

The live objects here are the renderings plus exactly the members a
Kubernetes 1.35 API server added on a server dry run of each roster object.
Every other difference, a missing or recreated object, or a changed guard Pod
or route refuses.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Any

import pytest
from curie_worker import commissioning_control_installation as ci
from curie_worker import commissioning_control_observer as co
from curie_worker.commissioning_control_profile import GUARD_SERVER_LABEL

from .test_commissioning_control_installation import MATERIAL, OWNER, PROFILE
from .test_commissioning_control_profile import projection

GUARD_IMAGE_DIGEST = PROFILE.as_dict()["guard"]["image"].rsplit("@", 1)[1]
POD_UID = "50000000-0000-4000-8000-000000000001"
CONTAINER = "containerd://" + "a" * 64


def served(document: dict[str, Any], uid: str) -> dict[str, Any]:
    """The rendering as the API server returns it: the measured defaults only."""
    value = deepcopy(document)
    value["metadata"].update(
        uid=uid, resourceVersion="291", creationTimestamp="2026-10-22T22:01:05Z"
    )
    kind = value["kind"]
    if kind == "Namespace":
        value["metadata"]["labels"]["kubernetes.io/metadata.name"] = value["metadata"]["name"]
        value["spec"] = {"finalizers": ["kubernetes"]}
        value["status"] = {"phase": "Active"}
    elif kind == "Service":
        value["spec"].update(
            clusterIP="10.96.0.10",
            clusterIPs=["10.96.0.10"],
            internalTrafficPolicy="Cluster",
            ipFamilies=["IPv4"],
            ipFamilyPolicy="SingleStack",
            sessionAffinity="None",
        )
        value["status"] = {"loadBalancer": {}}
    elif kind == "Deployment":
        value["metadata"]["generation"] = 1
        value["metadata"]["annotations"] = {"deployment.kubernetes.io/revision": "1"}
        spec = value["spec"]
        spec.update(
            progressDeadlineSeconds=600,
            revisionHistoryLimit=10,
            strategy={
                "rollingUpdate": {"maxSurge": "25%", "maxUnavailable": "25%"},
                "type": "RollingUpdate",
            },
        )
        pod = spec["template"]["spec"]
        pod.update(
            dnsPolicy="ClusterFirst",
            restartPolicy="Always",
            schedulerName="default-scheduler",
            terminationGracePeriodSeconds=30,
        )
        container = pod["containers"][0]
        container["ports"][0]["protocol"] = "TCP"
        container["readinessProbe"].update(failureThreshold=3, successThreshold=1, timeoutSeconds=1)
        container.update(
            resources={},
            terminationMessagePath="/dev/termination-log",
            terminationMessagePolicy="File",
        )
        value["status"] = {"readyReplicas": 1, "replicas": 1}
    elif kind == "ValidatingWebhookConfiguration":
        value["metadata"]["generation"] = 1
    return value


ROSTER = ci.render_control_installation(
    PROFILE, projection=projection(), material=MATERIAL, labels=OWNER
)
INSTALLED = ci.InstalledControl(
    tuple((ci.object_key(d), f"00000000-0000-4000-8000-{i:012d}") for i, d in enumerate(ROSTER))
)


class Reader:
    def __init__(self) -> None:
        self.objects = {
            key: served(document, uid)
            for (key, uid), document in zip(INSTALLED.uids, ROSTER, strict=True)
        }
        self.server = {"major": "1", "minor": "35", "gitVersion": "v1.35.1"}
        template = deepcopy(ROSTER[4]["spec"]["template"])
        spec = template["spec"]
        spec.update(serviceAccountName="default", nodeName="node-a", dnsPolicy="ClusterFirst")
        self.guard = {
            "metadata": {
                "name": "acme-guard-6d4f-x2x9z",
                "namespace": "acme-control",
                "uid": POD_UID,
                "labels": dict(template["metadata"]["labels"]),
            },
            "spec": spec,
            "status": {
                "phase": "Running",
                "podIP": "10.244.0.9",
                "containerStatuses": [
                    {
                        "name": "guard",
                        "ready": True,
                        "restartCount": 0,
                        "containerID": CONTAINER,
                        "imageID": "registry.example.com/acme-guard@" + GUARD_IMAGE_DIGEST,
                        "state": {"running": {"startedAt": "2026-10-22T22:01:09Z"}},
                    }
                ],
            },
        }
        self.extra_pods: list[dict[str, Any]] = []
        self.slices = [
            {
                "ports": [{"name": "https", "port": 9443, "protocol": "TCP"}],
                "endpoints": [
                    {
                        "addresses": ["10.244.0.9"],
                        "conditions": {"ready": True, "serving": True, "terminating": False},
                        "targetRef": {
                            "kind": "Pod",
                            "namespace": "acme-control",
                            "name": "acme-guard-6d4f-x2x9z",
                            "uid": POD_UID,
                        },
                    }
                ],
            }
        ]

    def read(self, key: str) -> dict[str, Any] | None:
        value = self.objects.get(key)
        return None if value is None else deepcopy(value)

    def version(self) -> dict[str, Any]:
        return dict(self.server)

    def pods(self, namespace: str, labels: dict[str, str]) -> list[dict[str, Any]]:
        assert namespace == "acme-control"
        assert labels == {GUARD_SERVER_LABEL: PROFILE.digest[:32]}
        return deepcopy([self.guard, *self.extra_pods])

    def endpoint_slices(self, namespace: str, service: str) -> list[dict[str, Any]]:
        assert (namespace, service) == ("acme-control", "acme-guard")
        return deepcopy(self.slices)


def observer(reader: Reader) -> co.ControlObserver:
    return co.ControlObserver(
        reader, profile=PROFILE, projection=projection(), roster=ROSTER, installed=INSTALLED
    )


def observe(reader: Reader) -> str:
    return asyncio.run(observer(reader).observe())


KEYS = [key for key, _ in INSTALLED.uids]
WEBHOOK, DEPLOYMENT, SERVICE, SECRET = KEYS[5], KEYS[4], KEYS[3], KEYS[2]


def test_the_installed_roster_yields_a_stable_fact():
    reader = Reader()
    assert observer(reader).fact == co.FACT == "control_profile"
    first = observe(reader)
    assert first == observe(reader) and len(first) == 64


def _set(key, path, value):
    def mutate(reader: Reader) -> None:
        target = reader.objects[key]
        for member in path[:-1]:
            target = target[member]
        target[path[-1]] = value

    return mutate


def _webhook(member, value):
    def mutate(reader: Reader) -> None:
        reader.objects[WEBHOOK]["webhooks"][0][member] = value

    return mutate


def _pod(path, value):
    def mutate(reader: Reader) -> None:
        target = reader.guard
        for member in path[:-1]:
            target = target[member]
        target[path[-1]] = value

    return mutate


def _delete(key):
    def mutate(reader: Reader) -> None:
        del reader.objects[key]

    return mutate


def _second_pod(reader: Reader) -> None:
    reader.extra_pods.append(deepcopy(reader.guard))


def _second_endpoint(reader: Reader) -> None:
    endpoint = deepcopy(reader.slices[0]["endpoints"][0])
    endpoint["addresses"] = ["10.244.0.10"]
    reader.slices[0]["endpoints"].append(endpoint)


def _endpoint(member, value):
    def mutate(reader: Reader) -> None:
        reader.slices[0]["endpoints"][0][member] = value

    return mutate


def _server(version):
    def mutate(reader: Reader) -> None:
        reader.server = version

    return mutate


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(_delete(WEBHOOK), id="webhook-removed"),
        pytest.param(
            _set(WEBHOOK, ("metadata", "uid"), "0" * 8 + "-0000-4000-8000-" + "9" * 12),
            id="webhook-recreated",
        ),
        pytest.param(_webhook("failurePolicy", "Ignore"), id="webhook-fails-open"),
        pytest.param(
            _webhook("matchConditions", [{"name": "n", "expression": "false"}]),
            id="webhook-narrowed",
        ),
        pytest.param(_webhook("namespaceSelector", {}), id="webhook-selector-changed"),
        pytest.param(_webhook("rules", []), id="webhook-rules-dropped"),
        pytest.param(
            _set(WEBHOOK, ("metadata", "deletionTimestamp"), "2026-10-22T23:00:00Z"),
            id="webhook-deleting",
        ),
        pytest.param(_delete(KEYS[0]), id="workload-namespace-removed"),
        pytest.param(_set(SECRET, ("data", "tls.key"), "eA=="), id="config-changed"),
        pytest.param(_set(SECRET, ("immutable",), False), id="config-mutable"),
        pytest.param(
            _set(SERVICE, ("spec", "selector"), {"app": "other"}), id="service-retargeted"
        ),
        pytest.param(
            _set(SERVICE, ("spec", "sessionAffinity"), "ClientIP"), id="service-default-changed"
        ),
        pytest.param(_set(DEPLOYMENT, ("spec", "replicas"), 2), id="guard-scaled"),
        pytest.param(
            _set(DEPLOYMENT, ("spec", "template", "spec", "hostNetwork"), True),
            id="guard-unknown-member",
        ),
        pytest.param(
            _set(DEPLOYMENT, ("spec", "template", "spec", "dnsPolicy"), "Default"),
            id="guard-default-changed",
        ),
        pytest.param(
            _set(DEPLOYMENT, ("metadata", "annotations"), {"x": "y"}), id="guard-annotated"
        ),
        pytest.param(
            _server({"major": "1", "minor": "33", "gitVersion": "v1.33.4"}), id="unpinned-minor"
        ),
        pytest.param(_second_pod, id="two-guard-pods"),
        pytest.param(
            _pod(("spec", "containers", 0, "image"), "registry.example.com/x@sha256:" + "d" * 64),
            id="guard-pod-image",
        ),
        pytest.param(
            _pod(("status", "containerStatuses", 0, "imageID"), "x@sha256:" + "d" * 64),
            id="guard-pod-image-id",
        ),
        pytest.param(
            _pod(("status", "containerStatuses", 0, "ready"), False), id="guard-not-ready"
        ),
        pytest.param(
            _pod(("status", "containerStatuses", 0, "restartCount"), 1), id="guard-restarted"
        ),
        pytest.param(
            _pod(("spec", "ephemeralContainers"), [{"name": "debug"}]), id="guard-ephemeral"
        ),
        pytest.param(_pod(("spec", "automountServiceAccountToken"), True), id="guard-token"),
        pytest.param(
            _pod(("metadata", "deletionTimestamp"), "2026-10-22T23:00:00Z"), id="guard-deleting"
        ),
        pytest.param(_second_endpoint, id="second-endpoint"),
        pytest.param(_endpoint("targetRef", {"kind": "Pod", "uid": "x"}), id="foreign-endpoint"),
        pytest.param(_endpoint("conditions", {"ready": False}), id="endpoint-not-ready"),
        pytest.param(_endpoint("addresses", ["10.244.0.99"]), id="endpoint-address"),
    ],
)
def test_any_drift_refuses(mutate):
    reader = Reader()
    mutate(reader)
    with pytest.raises(ci.ControlInstallationRefused) as caught:
        observe(reader)
    assert caught.value.__cause__ is None and str(caught.value) == (
        "Protected control installation refused"
    )


def test_a_replaced_guard_pod_changes_the_fact():
    reader = Reader()
    first = observe(reader)
    replacement = "50000000-0000-4000-8000-000000000002"
    reader.guard["metadata"]["uid"] = replacement
    reader.slices[0]["endpoints"][0]["targetRef"]["uid"] = replacement
    assert observe(reader) != first


def test_the_observer_binds_the_installed_roster():
    reader = Reader()
    with pytest.raises(ci.ControlInstallationRefused):
        co.ControlObserver(
            reader,
            profile=PROFILE,
            projection=projection(),
            roster=ROSTER[:-1],
            installed=INSTALLED,
        )
    with pytest.raises(ci.ControlInstallationRefused):
        co.ControlObserver(
            reader,
            profile=PROFILE,
            projection=projection(),
            roster=ROSTER,
            installed=ci.InstalledControl(INSTALLED.uids[::-1]),
        )
