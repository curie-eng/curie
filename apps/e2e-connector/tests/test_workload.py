"""deploy, run, logs and events against a fake test cluster API (#3247).

The fake answers at the ``ClusterApi`` seam with Kubernetes shapes from the API
reference (https://kubernetes.io/docs/reference/kubernetes-api/) and the API
concepts page (https://kubernetes.io/docs/reference/using-api/api-concepts/):

* Namespace ``metadata.labels`` and ``status.phase``
  (https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/namespace-v1/).
* Discovery: ``GET /api/v1`` and ``GET /apis/<group>/<version>`` answer an
  ``APIResourceList`` whose ``resources[]`` carry ``name`` (the plural, or
  ``plural/subresource``), ``kind`` and ``namespaced``; an unserved group
  version answers 404
  (https://kubernetes.io/docs/reference/using-api/api-concepts/#discovery-api).
* Object paths ``/api/v1/namespaces/<ns>/<plural>/<name>`` for the core group
  and ``/apis/<group>/<version>/namespaces/<ns>/<plural>/<name>`` otherwise;
  POST to the collection answers 201, PUT to the item answers 200, and an
  update carries ``metadata.resourceVersion``
  (https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-uris and
  https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions).
* A failed write answers a ``Status`` object with ``message``, ``reason`` and
  ``code`` (https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/status/).
* Job ``status.conditions`` of type Complete or Failed
  (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/job-v1/#JobStatus);
  DELETE with ``propagationPolicy=Background``
  (https://kubernetes.io/docs/reference/kubernetes-api/common-parameters/common-parameters/#propagationPolicy).
* Pods listed with ``labelSelector=job-name=<job>``, and
  ``status.containerStatuses[].state`` as one of ``waiting{reason}``,
  ``running{}`` or ``terminated{exitCode, reason, message}``
  (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodStatus).
* Pod log at ``/api/v1/namespaces/<ns>/pods/<pod>/log`` answers text/plain and
  takes ``container`` and ``tailLines``
  (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#get-read-log-of-the-specified-pod).
* Core v1 EventList items with ``reason``, ``message``, ``type``, ``count`` and
  ``series.count``; ``fieldSelector=involvedObject.name=<x>`` filters them
  (https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/).
"""

from __future__ import annotations

import json
import urllib.parse
from typing import Any

import httpx
import pytest
import yaml
from curie_e2e_connector import contract
from curie_e2e_connector.contract import (
    BUILD_LABEL,
    OUTPUT_LIMIT_BYTES,
    OWNER_LABEL,
    POD_SECURITY_LABEL,
    REFUSAL_CLUSTER_SCOPED,
    REFUSAL_DEPLOY_FAILED,
    REFUSAL_DEPLOY_MANIFEST,
    REFUSAL_DEPLOY_OBJECT,
    REFUSAL_ENVIRONMENT_REQUIRED,
    REFUSAL_IMAGE_NOT_DIGEST,
    REFUSAL_LOGS_REFUSED,
    REFUSAL_NOT_OWNED,
    REFUSAL_POD_NOT_FOUND,
    REFUSAL_RUN_ARGUMENT,
    REFUSAL_RUN_FAILED,
    REFUSAL_RUN_TIMEOUT,
    RUN_JOB_PREFIX,
    RUN_LABEL,
    RUN_TIMEOUT_S,
    WORK_ITEM_LABEL,
)
from curie_e2e_connector.kube import ClusterApi, ClusterError, HttpxCluster
from curie_e2e_connector.namespace import Caller, Install, require_caller
from curie_e2e_connector.workload import deploy, list_events, read_logs, run_command

RUN = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
OTHER_RUN = "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"
WORK = "11111111-2222-4333-8444-555555555555"
NS = f"curie-e2e-{RUN}"
OTHER_NS = "kube-system"
HEX = "ab" * 32
IMAGE = f"registry.test.example/e2e/app@sha256:{HEX}"
INIT_IMAGE = f"registry.test.example/e2e/init@sha256:{'cd' * 32}"
RUN_ID = "0a1b2c3d"
RUN_JOB = f"{RUN_JOB_PREFIX}{RUN_ID}"
RUN_LABELS = {OWNER_LABEL: "acme", RUN_LABEL: RUN, WORK_ITEM_LABEL: WORK}
WRITES = ("POST", "PUT", "PATCH", "DELETE")

CORE = f"/api/v1/namespaces/{NS}"
APPS = f"/apis/apps/v1/namespaces/{NS}"
BATCH = f"/apis/batch/v1/namespaces/{NS}"
RBAC = f"/apis/rbac.authorization.k8s.io/v1/namespaces/{NS}"


def install(**overrides: Any) -> Install:
    values: dict[str, Any] = {
        "namespace_prefix": "curie-e2e-",
        "owner_label_key": OWNER_LABEL,
        "owner_label_value": "acme",
        "service_account": "curie-e2e-connector",
        "service_account_namespace": "test-system",
        "worker_cluster_role": "curie-e2e-connector-namespace",
        "ttl_seconds": 3600,
        "pod_security": "baseline",
    }
    values.update(overrides)
    return Install(**values)


def caller() -> Caller:
    return require_caller(RUN, WORK)


def namespace_object(*, run: str = RUN, phase: str = "Active") -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": NS,
            "labels": {
                OWNER_LABEL: "acme",
                RUN_LABEL: run,
                WORK_ITEM_LABEL: WORK,
                POD_SECURITY_LABEL: "baseline",
            },
        },
        "status": {"phase": phase},
    }


def resource(name: str, kind: str, *, namespaced: bool = True) -> dict[str, Any]:
    return {"name": name, "singularName": "", "namespaced": namespaced, "kind": kind}


# APIResourceList per group version. Subresources (``deployments/status``) sit
# beside their parent, as the apiserver lists them.
DISCOVERY: dict[str, list[dict[str, Any]]] = {
    "v1": [
        resource("configmaps", "ConfigMap"),
        resource("events", "Event"),
        resource("limitranges", "LimitRange"),
        resource("namespaces", "Namespace", namespaced=False),
        resource("nodes", "Node", namespaced=False),
        resource("persistentvolumeclaims", "PersistentVolumeClaim"),
        resource("pods", "Pod"),
        resource("pods/log", "Pod"),
        resource("pods/status", "Pod"),
        resource("resourcequotas", "ResourceQuota"),
        resource("secrets", "Secret"),
        resource("serviceaccounts", "ServiceAccount"),
        resource("services", "Service"),
        resource("services/status", "Service"),
    ],
    "apps/v1": [
        resource("daemonsets", "DaemonSet"),
        resource("deployments", "Deployment"),
        resource("deployments/scale", "Scale"),
        resource("deployments/status", "Deployment"),
        resource("replicasets", "ReplicaSet"),
        resource("statefulsets", "StatefulSet"),
    ],
    "batch/v1": [
        resource("cronjobs", "CronJob"),
        resource("jobs", "Job"),
        resource("jobs/status", "Job"),
    ],
    "networking.k8s.io/v1": [
        resource("ingressclasses", "IngressClass", namespaced=False),
        resource("ingresses", "Ingress"),
        resource("networkpolicies", "NetworkPolicy"),
    ],
    "rbac.authorization.k8s.io/v1": [
        resource("clusterrolebindings", "ClusterRoleBinding", namespaced=False),
        resource("clusterroles", "ClusterRole", namespaced=False),
        resource("rolebindings", "RoleBinding"),
        resource("roles", "Role"),
    ],
    "scheduling.k8s.io/v1": [resource("priorityclasses", "PriorityClass", namespaced=False)],
    "admissionregistration.k8s.io/v1": [
        resource("mutatingwebhookconfigurations", "MutatingWebhookConfiguration", namespaced=False),
        resource(
            "validatingwebhookconfigurations", "ValidatingWebhookConfiguration", namespaced=False
        ),
    ],
    "apiextensions.k8s.io/v1": [
        resource("customresourcedefinitions", "CustomResourceDefinition", namespaced=False),
        resource("customresourcedefinitions/status", "CustomResourceDefinition", namespaced=False),
    ],
    # A CRD-defined group: one namespaced kind, one cluster scoped kind.
    "example.com/v1": [
        resource("gadgets", "Gadget", namespaced=False),
        resource("widgets", "Widget"),
        resource("widgets/status", "Widget"),
    ],
}


def not_found() -> tuple[int, dict[str, Any]]:
    return 404, {"kind": "Status", "status": "Failure", "reason": "NotFound", "code": 404}


def condition(kind: str, *, reason: str = "", message: str = "") -> dict[str, Any]:
    return {"type": kind, "status": "True", "reason": reason, "message": message}


ACTIVE: dict[str, Any] = {"active": 1}
COMPLETE: dict[str, Any] = {"succeeded": 1, "conditions": [condition("Complete")]}
FAILED: dict[str, Any] = {
    "failed": 1,
    "conditions": [condition("Failed", reason="BackoffLimitExceeded", message="Job has failed")],
}


def run_pod(state: dict[str, Any], name: str = f"{RUN_JOB}-x7k2p") -> dict[str, Any]:
    return {
        "metadata": {"name": name, "namespace": NS, "labels": {"job-name": RUN_JOB}},
        "status": {"containerStatuses": [{"name": "run", "ready": False, "state": state}]},
    }


def waiting(reason: str) -> dict[str, Any]:
    return {"waiting": {"reason": reason, "message": f"{reason} detail"}}


RUNNING: dict[str, Any] = {"running": {"startedAt": "2026-10-03T12:00:00Z"}}


def terminated(code: int, *, message: str = "") -> dict[str, Any]:
    state: dict[str, Any] = {"exitCode": code, "reason": "Completed" if code == 0 else "Error"}
    if message:
        state["message"] = message
    return {"terminated": state}


class FakeCluster(ClusterApi):
    """One run namespace, discovery, existing objects, run Jobs, pod logs and events."""

    def __init__(self, *, namespace: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.text_calls: list[str] = []
        self.namespace = namespace if namespace is not None else namespace_object()
        # Item path to the object the apiserver holds.
        self.existing: dict[str, dict[str, Any]] = {}
        # Collection path to a forced POST answer.
        self.post_answers: dict[str, tuple[int, dict[str, Any]]] = {}
        # Run Jobs: per-poll Job status, and the pod state at that poll.
        self.job_script: list[dict[str, Any]] = [ACTIVE, COMPLETE]
        self.pod_script: list[dict[str, Any]] = [RUNNING, terminated(0)]
        self.job_gets = 0
        self.logs: dict[str, str] = {f"{RUN_JOB}-x7k2p": "hello from run\n"}
        # Forced answers for the run pod list and the run pod log read.
        self.pod_list_answer: tuple[int, dict[str, Any]] | None = None
        self.log_answer: tuple[int, str] | None = None
        self.events: list[dict[str, Any]] = []

    # Call log views

    def writes(self) -> list[tuple[str, str, dict[str, Any] | None]]:
        return [call for call in self.calls if call[0] in WRITES]

    def posts(self) -> list[tuple[str, dict[str, Any]]]:
        return [(path, body) for method, path, body in self.calls if method == "POST" and body]

    def puts(self) -> list[tuple[str, dict[str, Any]]]:
        return [(path, body) for method, path, body in self.calls if method == "PUT" and body]

    def job_deletes(self) -> list[tuple[str, dict[str, str]]]:
        out = []
        for method, path, _body in self.calls:
            parsed = urllib.parse.urlsplit(path)
            if method == "DELETE" and parsed.path.startswith(f"{BATCH}/jobs/"):
                out.append((parsed.path, dict(urllib.parse.parse_qsl(parsed.query))))
        return out

    # The API

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        if len(self.calls) > 5000:
            raise AssertionError("the workload never stopped calling the cluster")
        parsed = urllib.parse.urlsplit(path)
        bare = parsed.path
        query = dict(urllib.parse.parse_qsl(parsed.query))

        if method == "GET" and bare == CORE:
            if self.namespace is None:
                return not_found()
            return 200, self.namespace

        if method == "GET" and bare == "/api/v1":
            return 200, {
                "kind": "APIResourceList",
                "groupVersion": "v1",
                "resources": DISCOVERY["v1"],
            }
        if method == "GET" and bare.startswith("/apis/") and bare.count("/") == 3:
            group_version = bare.removeprefix("/apis/")
            if group_version not in DISCOVERY:
                return not_found()
            return 200, {
                "kind": "APIResourceList",
                "groupVersion": group_version,
                "resources": DISCOVERY[group_version],
            }

        if method == "GET" and bare == f"{BATCH}/jobs/{RUN_JOB}" and bare not in self.existing:
            self.job_gets += 1
            status = self.job_script[min(self.job_gets - 1, len(self.job_script) - 1)]
            return 200, {"kind": "Job", "metadata": {"name": RUN_JOB}, "status": status}

        if method == "GET" and bare == f"{CORE}/pods" and "labelSelector" in query:
            if self.pod_list_answer is not None:
                return self.pod_list_answer
            job = query["labelSelector"].removeprefix("job-name=")
            if job != RUN_JOB or self.job_gets == 0:
                return 200, {"kind": "PodList", "items": []}
            state = self.pod_script[min(self.job_gets - 1, len(self.pod_script) - 1)]
            return 200, {"kind": "PodList", "items": [run_pod(state)]}

        if method == "GET" and bare == f"{CORE}/events":
            items = self.events
            selector = query.get("fieldSelector")
            if selector:
                name = selector.removeprefix("involvedObject.name=")
                items = [e for e in items if e["involvedObject"]["name"] == name]
            return 200, {"kind": "EventList", "apiVersion": "v1", "items": items}

        if method == "GET":
            if bare in self.existing:
                return 200, self.existing[bare]
            return not_found()

        if method == "POST":
            assert body is not None
            if bare in self.post_answers:
                return self.post_answers[bare]
            return 201, body

        if method == "PUT":
            assert body is not None
            return 200, body

        if method == "DELETE":
            return 200, {"kind": "Status", "status": "Success"}

        return 500, {"kind": "Status", "code": 500}

    def read_text(self, path: str) -> tuple[int, str]:
        self.text_calls.append(path)
        bare = urllib.parse.urlsplit(path).path
        prefix = f"{CORE}/pods/"
        if bare.startswith(prefix) and bare.endswith("/log"):
            if self.log_answer is not None:
                return self.log_answer
            pod = bare.removeprefix(prefix).removesuffix("/log")
            if pod in self.logs:
                return 200, self.logs[pod]
        return 404, "not found"


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def refusal(exc: pytest.ExceptionInfo[ClusterError]) -> tuple[str, str]:
    """The machine readable code and the text after it."""

    code, _sep, tail = str(exc.value).partition(": ")
    return code, tail


def refusal_json(exc: pytest.ExceptionInfo[ClusterError]) -> dict[str, Any]:
    _code, tail = refusal(exc)
    loaded = json.loads(tail)
    assert isinstance(loaded, dict)
    return loaded


def manifest(*docs: dict[str, Any]) -> str:
    return yaml.safe_dump_all(list(docs), sort_keys=False)


def run_deploy(cluster: FakeCluster, text: str) -> dict[str, Any]:
    return deploy(cluster, install(), caller(), text)


def refuse_deploy(cluster: FakeCluster, text: str, code: str) -> pytest.ExceptionInfo[ClusterError]:
    with pytest.raises(ClusterError) as exc:
        run_deploy(cluster, text)
    assert refusal(exc)[0] == code, str(exc.value)
    assert cluster.writes() == []
    return exc


def run(
    cluster: FakeCluster,
    *,
    command: list[Any] | None = None,
    image: str = IMAGE,
    clock: FakeClock | None = None,
) -> dict[str, Any]:
    tick = clock or FakeClock()
    return run_command(
        cluster,
        install(),
        caller(),
        command if command is not None else ["sh", "-c", "curl -fsS http://web/healthz"],
        image,
        clock=tick.clock,
        sleep=tick.sleep,
        run_id=RUN_ID,
        poll_seconds=5,
    )


# Object builders


def container(name: str, image: str | None = IMAGE) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name}
    if image is not None:
        out["image"] = image
    return out


def deployment(
    name: str = "web",
    *,
    image: str = IMAGE,
    template_labels: dict[str, str] | None = None,
    **metadata: Any,
) -> dict[str, Any]:
    labels = template_labels or {"app": name}
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "labels": {"app": name}, **metadata},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "initContainers": [container("migrate", INIT_IMAGE)],
                    "containers": [container(name, image)],
                },
            },
        },
    }


def service(name: str = "web") -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name},
        "spec": {"selector": {"app": name}, "ports": [{"port": 80, "targetPort": 8080}]},
    }


def configmap(name: str = "web-config") -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name},
        "data": {"LOG_LEVEL": "debug"},
    }


def secret(name: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name},
        "type": "Opaque",
        "stringData": {"key": "value"},
    }


def pod(name: str = "probe", **spec: Any) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [container("probe")], **spec},
    }


def job(name: str, *, labels: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, **({"labels": labels} if labels else {})},
        "spec": {
            "template": {"spec": {"restartPolicy": "Never", "containers": [container("job")]}}
        },
    }


def role(name: str, rules: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": name},
        "rules": rules,
    }


def rolebinding(name: str, role_name: str, *, role_kind: str = "Role") -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": name},
        "subjects": [{"kind": "ServiceAccount", "name": "default", "namespace": NS}],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": role_kind,
            "name": role_name,
        },
    }


def cluster_object(api_version: str, kind: str, name: str, **extra: Any) -> dict[str, Any]:
    return {"apiVersion": api_version, "kind": kind, "metadata": {"name": name}, **extra}


def crd() -> dict[str, Any]:
    return cluster_object(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        "sandboxes.agents.example.com",
        spec={
            "group": "agents.example.com",
            "scope": "Namespaced",
            "names": {"plural": "sandboxes", "kind": "Sandbox"},
            "versions": [{"name": "v1", "served": True, "storage": True}],
        },
    )


SECRETS_RULE = {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}
CONFIGMAP_RULE = {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "list"]}


# AC1: deploy by digest

MULTI_DOC = f"""\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels:
    app: web
spec:
  replicas: 1
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      initContainers:
        - name: migrate
          image: {INIT_IMAGE}
      containers:
        - name: web
          image: {IMAGE}
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  selector:
    app: web
  ports:
    - port: 80
      targetPort: 8080
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-config
data:
  LOG_LEVEL: debug
"""


def test_a_multi_document_manifest_by_digest_is_created_in_the_run_namespace() -> None:
    cluster = FakeCluster()

    result = run_deploy(cluster, MULTI_DOC)

    assert result == {
        "namespace": NS,
        "applied": [
            {"kind": "Deployment", "name": "web"},
            {"kind": "Service", "name": "web"},
            {"kind": "ConfigMap", "name": "web-config"},
        ],
    }
    posts = cluster.posts()
    assert [path for path, _body in posts] == [
        f"{APPS}/deployments",
        f"{CORE}/services",
        f"{CORE}/configmaps",
    ]
    for _path, body in posts:
        assert body["metadata"]["namespace"] == NS
        assert RUN_LABELS.items() <= body["metadata"]["labels"].items()
    deployment_body = posts[0][1]
    assert deployment_body["metadata"]["labels"]["app"] == "web"
    pod_spec = deployment_body["spec"]["template"]["spec"]
    assert pod_spec["containers"][0]["image"] == IMAGE
    assert pod_spec["initContainers"][0]["image"] == INIT_IMAGE
    assert cluster.puts() == []


def test_an_existing_object_is_replaced_with_its_resource_version() -> None:
    cluster = FakeCluster()
    cluster.existing[f"{APPS}/deployments/web"] = {
        **deployment(),
        "metadata": {"name": "web", "namespace": NS, "resourceVersion": "41", "uid": "live-uid"},
    }
    submitted = deployment()
    submitted["metadata"].update({"uid": "stale-uid", "resourceVersion": "7"})
    submitted["status"] = {"replicas": 1, "readyReplicas": 1}

    result = run_deploy(cluster, manifest(submitted))

    assert result["applied"] == [{"kind": "Deployment", "name": "web"}]
    [(path, body)] = cluster.puts()
    assert path == f"{APPS}/deployments/web"
    assert body["metadata"]["resourceVersion"] == "41"
    assert "uid" not in body["metadata"]
    assert "status" not in body
    assert body["metadata"]["namespace"] == NS
    assert RUN_LABELS.items() <= body["metadata"]["labels"].items()
    assert cluster.posts() == []


def test_a_namespaced_custom_resource_deploys_through_discovery() -> None:
    cluster = FakeCluster()
    widget = {
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "w1"},
        "spec": {"size": 3},
    }

    result = run_deploy(cluster, manifest(widget))

    assert result["applied"] == [{"kind": "Widget", "name": "w1"}]
    [(path, body)] = cluster.posts()
    assert path == f"/apis/example.com/v1/namespaces/{NS}/widgets"
    assert body["metadata"]["namespace"] == NS


def test_a_list_document_is_flattened_into_its_items() -> None:
    cluster = FakeCluster()
    listing = {"apiVersion": "v1", "kind": "List", "items": [configmap(), service()]}

    result = run_deploy(cluster, manifest(listing))

    assert result["applied"] == [
        {"kind": "ConfigMap", "name": "web-config"},
        {"kind": "Service", "name": "web"},
    ]
    assert [path for path, _body in cluster.posts()] == [
        f"{CORE}/configmaps",
        f"{CORE}/services",
    ]


def test_a_tagged_image_is_refused_naming_the_container_without_writes() -> None:
    cluster = FakeCluster()

    exc = refuse_deploy(
        cluster, manifest(deployment(image="nginx:1.27"), service()), REFUSAL_IMAGE_NOT_DIGEST
    )

    assert refusal_json(exc) == {
        "images": [{"kind": "Deployment", "name": "web", "container": "web", "image": "nginx:1.27"}]
    }


def cronjob(image: str) -> dict[str, Any]:
    return {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "nightly"},
        "spec": {
            "schedule": "0 0 * * *",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {"restartPolicy": "Never", "containers": [container("task", image)]}
                    }
                }
            },
        },
    }


@pytest.mark.parametrize(
    ("doc", "kind", "name", "container_name", "image"),
    [
        pytest.param(deployment(image="nginx"), "Deployment", "web", "web", "nginx", id="bare"),
        pytest.param(
            cronjob("busybox:1.36"), "CronJob", "nightly", "task", "busybox:1.36", id="cronjob"
        ),
        pytest.param(
            pod(initContainers=[container("init", "alpine:3.20")]),
            "Pod",
            "probe",
            "init",
            "alpine:3.20",
            id="pod-init",
        ),
        pytest.param(
            pod(ephemeralContainers=[container("debug", "busybox:latest")]),
            "Pod",
            "probe",
            "debug",
            "busybox:latest",
            id="pod-ephemeral",
        ),
    ],
)
def test_images_without_a_digest_are_refused_wherever_they_sit(
    doc: dict[str, Any], kind: str, name: str, container_name: str, image: str
) -> None:
    cluster = FakeCluster()

    exc = refuse_deploy(cluster, manifest(doc), REFUSAL_IMAGE_NOT_DIGEST)

    assert {"kind": kind, "name": name, "container": container_name, "image": image} in (
        refusal_json(exc)["images"]
    )


def test_a_tag_pinned_by_a_digest_is_accepted() -> None:
    cluster = FakeCluster()
    pinned = f"registry.test.example/e2e/app:1.0@sha256:{HEX}"

    result = run_deploy(cluster, manifest(deployment(image=pinned)))

    assert result["applied"] == [{"kind": "Deployment", "name": "web"}]
    [(_path, body)] = cluster.posts()
    assert body["spec"]["template"]["spec"]["containers"][0]["image"] == pinned


def test_an_object_naming_another_namespace_is_refused_without_writes() -> None:
    cluster = FakeCluster()
    refuse_deploy(cluster, manifest(deployment(namespace=OTHER_NS)), REFUSAL_NOT_OWNED)


def test_a_missing_namespace_is_refused_without_writes() -> None:
    cluster = FakeCluster()
    cluster.namespace = None  # type: ignore[assignment]
    refuse_deploy(cluster, manifest(deployment()), REFUSAL_ENVIRONMENT_REQUIRED)


def test_a_namespace_of_another_run_is_refused_without_writes() -> None:
    cluster = FakeCluster(namespace=namespace_object(run=OTHER_RUN))
    refuse_deploy(cluster, manifest(deployment()), REFUSAL_NOT_OWNED)


def allow_all_policy(name: str) -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name},
        "spec": {"podSelector": {}, "egress": [{}], "policyTypes": ["Egress"]},
    }


PROTECTED = [
    pytest.param(
        [deployment(template_labels={"app": "web", BUILD_LABEL: "e2e-build-0a1b2c3d"})],
        "Deployment",
        "web",
        id="build-label-on-template",
    ),
    pytest.param([secret("e2e-registry-push-abc")], "Secret", "e2e-registry-push-abc", id="secret"),
    pytest.param(
        [pod(volumes=[{"name": "creds", "secret": {"secretName": "e2e-registry-push-abc"}}])],
        "Pod",
        "probe",
        id="volume-secret-ref",
    ),
    pytest.param(
        [{**configmap("e2e-images"), "data": {"ledger.json": "{}"}}],
        "ConfigMap",
        "e2e-images",
        id="images-ledger",
    ),
    pytest.param(
        [allow_all_policy("e2e-build-egress")],
        "NetworkPolicy",
        "e2e-build-egress",
        id="build-egress-policy",
    ),
    pytest.param(
        [allow_all_policy("allow-everything")],
        "NetworkPolicy",
        "allow-everything",
        id="any-network-policy",
    ),
    pytest.param(
        [
            {
                "apiVersion": "v1",
                "kind": "ResourceQuota",
                "metadata": {"name": "bigger"},
                "spec": {"hard": {"pods": "1000"}},
            }
        ],
        "ResourceQuota",
        "bigger",
        id="resource-quota",
    ),
    pytest.param(
        [
            {
                "apiVersion": "v1",
                "kind": "LimitRange",
                "metadata": {"name": "looser"},
                "spec": {"limits": [{"type": "Container", "max": {"cpu": "64"}}]},
            }
        ],
        "LimitRange",
        "looser",
        id="limit-range",
    ),
    pytest.param(
        [rolebinding("e2e-connector", "app-reader")],
        "RoleBinding",
        "e2e-connector",
        id="connector-rolebinding",
    ),
    pytest.param([role("reader", [SECRETS_RULE])], "Role", "reader", id="role-secrets"),
    pytest.param(
        [role("star", [{"apiGroups": [""], "resources": ["*"], "verbs": ["get"]}])],
        "Role",
        "star",
        id="role-wildcard-resources",
    ),
    pytest.param(
        [
            role(
                "binder",
                [
                    {
                        "apiGroups": ["rbac.authorization.k8s.io"],
                        "resources": ["rolebindings"],
                        "verbs": ["create"],
                    }
                ],
            )
        ],
        "Role",
        "binder",
        id="role-rolebindings",
    ),
    pytest.param(
        [rolebinding("admin-binding", "admin", role_kind="ClusterRole")],
        "RoleBinding",
        "admin-binding",
        id="rolebinding-clusterrole",
    ),
    pytest.param(
        [role("reader", [SECRETS_RULE]), rolebinding("reader-binding", "reader")],
        "RoleBinding",
        "reader-binding",
        id="rolebinding-to-manifest-role",
    ),
]


@pytest.mark.parametrize(("docs", "kind", "name"), PROTECTED)
def test_protected_objects_are_refused_without_writes(
    docs: list[dict[str, Any]], kind: str, name: str
) -> None:
    cluster = FakeCluster()

    exc = refuse_deploy(cluster, manifest(*docs), REFUSAL_DEPLOY_OBJECT)

    objects = refusal_json(exc)["objects"]
    assert any(o["kind"] == kind and o["name"] == name and o["reason"] for o in objects), objects


def test_a_rolebinding_to_an_existing_role_that_reads_secrets_is_refused() -> None:
    cluster = FakeCluster()
    cluster.existing[f"{RBAC}/roles/reader"] = {
        **role("reader", [SECRETS_RULE]),
        "metadata": {"name": "reader", "namespace": NS, "resourceVersion": "5"},
    }

    exc = refuse_deploy(
        cluster, manifest(rolebinding("reader-binding", "reader")), REFUSAL_DEPLOY_OBJECT
    )

    assert [(o["kind"], o["name"]) for o in refusal_json(exc)["objects"]] == [
        ("RoleBinding", "reader-binding")
    ]


# A deployed Role may grant only get, list and watch, never on secrets. Every
# other verb is a write or a privilege verb (escalate, bind, impersonate), and
# a subresource such as pods/exec is checked like any resource
# (https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources).
WRITE_VERB_ROLES = [
    pytest.param(
        {"apiGroups": ["networking.k8s.io"], "resources": ["networkpolicies"], "verbs": ["create"]},
        id="networkpolicies-create",
    ),
    pytest.param({"apiGroups": [""], "resources": ["pods"], "verbs": ["create"]}, id="pods-create"),
    pytest.param(
        {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["create"]}, id="pods-exec-create"
    ),
    pytest.param({"apiGroups": [""], "resources": ["pods"], "verbs": ["patch"]}, id="pods-patch"),
    pytest.param(
        {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["delete"]}, id="jobs-delete"
    ),
    pytest.param(
        {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["*"]}, id="configmaps-star"
    ),
]


@pytest.mark.parametrize("rule", WRITE_VERB_ROLES)
def test_a_role_granting_any_verb_beyond_read_is_refused(rule: dict[str, Any]) -> None:
    cluster = FakeCluster()

    exc = refuse_deploy(
        cluster,
        manifest(role("writer", [CONFIGMAP_RULE, rule]), rolebinding("writer-binding", "writer")),
        REFUSAL_DEPLOY_OBJECT,
    )

    objects = refusal_json(exc)["objects"]
    assert any(o["kind"] == "Role" and o["name"] == "writer" and o["reason"] for o in objects)


def test_a_rolebinding_to_an_existing_role_granting_pods_exec_is_refused() -> None:
    cluster = FakeCluster()
    exec_rule = {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["create"]}
    cluster.existing[f"{RBAC}/roles/shell"] = {
        **role("shell", [exec_rule]),
        "metadata": {"name": "shell", "namespace": NS, "resourceVersion": "5"},
    }

    exc = refuse_deploy(
        cluster, manifest(rolebinding("shell-binding", "shell")), REFUSAL_DEPLOY_OBJECT
    )

    assert [(o["kind"], o["name"]) for o in refusal_json(exc)["objects"]] == [
        ("RoleBinding", "shell-binding")
    ]


def test_a_read_only_role_over_configmaps_and_pod_logs_deploys() -> None:
    cluster = FakeCluster()
    rules = [
        {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get", "list", "watch"]},
    ]

    result = run_deploy(
        cluster, manifest(role("observer", rules), rolebinding("observer-binding", "observer"))
    )

    assert result["applied"] == [
        {"kind": "Role", "name": "observer"},
        {"kind": "RoleBinding", "name": "observer-binding"},
    ]
    assert [path for path, _body in cluster.posts()] == [f"{RBAC}/roles", f"{RBAC}/rolebindings"]


def test_a_job_that_would_replace_a_build_job_is_refused() -> None:
    cluster = FakeCluster()
    build_job = "e2e-build-0a1b2c3d"
    cluster.existing[f"{BATCH}/jobs/{build_job}"] = {
        **job(build_job, labels={BUILD_LABEL: build_job}),
        "metadata": {
            "name": build_job,
            "namespace": NS,
            "resourceVersion": "9",
            "labels": {BUILD_LABEL: build_job},
        },
    }

    exc = refuse_deploy(cluster, manifest(job(build_job)), REFUSAL_DEPLOY_OBJECT)

    assert [(o["kind"], o["name"]) for o in refusal_json(exc)["objects"]] == [("Job", build_job)]


def test_a_configmap_only_role_and_its_bindings_deploy() -> None:
    cluster = FakeCluster()
    docs = [
        role("config-reader", [CONFIGMAP_RULE]),
        rolebinding("config-reader-binding", "config-reader"),
        rolebinding("future-binding", "not-created-yet"),
    ]

    result = run_deploy(cluster, manifest(*docs))

    assert result["applied"] == [
        {"kind": "Role", "name": "config-reader"},
        {"kind": "RoleBinding", "name": "config-reader-binding"},
        {"kind": "RoleBinding", "name": "future-binding"},
    ]
    assert [path for path, _body in cluster.posts()] == [
        f"{RBAC}/roles",
        f"{RBAC}/rolebindings",
        f"{RBAC}/rolebindings",
    ]


def test_an_application_secret_deploys() -> None:
    cluster = FakeCluster()

    result = run_deploy(cluster, manifest(secret("app-config")))

    assert result["applied"] == [{"kind": "Secret", "name": "app-config"}]
    assert [path for path, _body in cluster.posts()] == [f"{CORE}/secrets"]


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("kind: [unclosed\n", id="malformed-yaml"),
        pytest.param(manifest({"apiVersion": "v1", "metadata": {"name": "x"}}), id="no-kind"),
        pytest.param("- a\n- b\n", id="not-a-mapping"),
    ],
)
def test_unreadable_manifests_are_refused_without_writes(text: str) -> None:
    refuse_deploy(FakeCluster(), text, REFUSAL_DEPLOY_MANIFEST)


# AC2: run


def run_job_body(cluster: FakeCluster) -> dict[str, Any]:
    [(path, body)] = cluster.posts()
    assert path == f"{BATCH}/jobs"
    return body


def assert_run_job_deleted(cluster: FakeCluster) -> None:
    deletes = cluster.job_deletes()
    assert deletes, "the run Job was not deleted"
    assert all(path == f"{BATCH}/jobs/{RUN_JOB}" for path, _query in deletes)
    assert deletes[-1][1].get("propagationPolicy") == "Background"


def test_run_creates_a_one_shot_job_and_returns_its_output() -> None:
    cluster = FakeCluster()
    command = ["sh", "-c", "curl -fsS http://web/healthz"]

    result = run(cluster, command=command)

    assert result == {"exit_code": 0, "stdout": "hello from run\n", "stderr": ""}
    body = run_job_body(cluster)
    assert body["kind"] == "Job"
    assert body["metadata"]["name"] == RUN_JOB
    assert RUN_LABELS.items() <= body["metadata"]["labels"].items()
    assert body["spec"]["backoffLimit"] == 0
    assert body["spec"]["activeDeadlineSeconds"] == RUN_TIMEOUT_S
    pod_spec = body["spec"]["template"]["spec"]
    assert pod_spec["restartPolicy"] == "Never"
    assert pod_spec["automountServiceAccountToken"] is False
    [run_container] = pod_spec["containers"]
    assert run_container["name"] == "run"
    assert run_container["command"] == command
    assert run_container["image"] == IMAGE
    assert run_container["securityContext"]["allowPrivilegeEscalation"] is False
    [log_path] = cluster.text_calls
    assert urllib.parse.urlsplit(log_path).path == f"{CORE}/pods/{RUN_JOB}-x7k2p/log"
    assert_run_job_deleted(cluster)


def test_a_non_zero_exit_is_a_result_not_an_error() -> None:
    cluster = FakeCluster()
    cluster.job_script = [ACTIVE, FAILED]
    cluster.pod_script = [RUNNING, terminated(3, message="boom")]

    result = run(cluster)

    assert result["exit_code"] == 3
    assert "boom" in result["stderr"]
    assert result["stdout"] == "hello from run\n"
    assert_run_job_deleted(cluster)


def test_a_pod_still_creating_keeps_waiting_until_it_completes() -> None:
    cluster = FakeCluster()
    cluster.job_script = [ACTIVE, ACTIVE, ACTIVE, COMPLETE]
    cluster.pod_script = [
        waiting("ContainerCreating"),
        waiting("ContainerCreating"),
        RUNNING,
        terminated(0),
    ]
    tick = FakeClock()

    result = run(cluster, clock=tick)

    assert result["exit_code"] == 0
    assert len(tick.sleeps) >= 2
    assert_run_job_deleted(cluster)


def test_one_err_image_pull_then_success_does_not_fail() -> None:
    cluster = FakeCluster()
    cluster.job_script = [ACTIVE, ACTIVE, COMPLETE]
    cluster.pod_script = [waiting("ErrImagePull"), RUNNING, terminated(0)]

    assert run(cluster)["exit_code"] == 0


def test_image_pull_back_off_fails_fast_and_deletes_the_job() -> None:
    cluster = FakeCluster()
    cluster.job_script = [ACTIVE]
    cluster.pod_script = [waiting("ImagePullBackOff")]
    tick = FakeClock()

    with pytest.raises(ClusterError) as exc:
        run(cluster, clock=tick)

    assert refusal(exc)[0] == REFUSAL_RUN_FAILED
    assert tick.now - 1000.0 < 60
    assert_run_job_deleted(cluster)


def test_a_run_past_its_deadline_times_out_and_deletes_the_job() -> None:
    cluster = FakeCluster()
    cluster.job_script = [ACTIVE]
    cluster.pod_script = [RUNNING]
    tick = FakeClock()

    with pytest.raises(ClusterError) as exc:
        run(cluster, clock=tick)

    assert refusal(exc)[0] == REFUSAL_RUN_TIMEOUT
    assert tick.now - 1000.0 >= RUN_TIMEOUT_S
    assert_run_job_deleted(cluster)


def test_run_refuses_a_tagged_image_before_creating_anything() -> None:
    cluster = FakeCluster()

    with pytest.raises(ClusterError) as exc:
        run(cluster, image="curlimages/curl:8.10.1")

    assert refusal(exc)[0] == REFUSAL_IMAGE_NOT_DIGEST
    assert cluster.writes() == []


@pytest.mark.parametrize(
    "command", [pytest.param([], id="empty"), pytest.param(["sh", 7], id="non-string")]
)
def test_run_refuses_a_bad_command_before_creating_anything(command: list[Any]) -> None:
    cluster = FakeCluster()

    with pytest.raises(ClusterError) as exc:
        run(cluster, command=command)

    assert refusal(exc)[0] == REFUSAL_RUN_ARGUMENT
    assert cluster.writes() == []


def test_run_output_keeps_only_the_last_output_limit_bytes() -> None:
    cluster = FakeCluster()
    text = "x" * 1000 + "y" * OUTPUT_LIMIT_BYTES
    cluster.logs[f"{RUN_JOB}-x7k2p"] = text

    result = run(cluster)

    assert result["stdout"] == "y" * OUTPUT_LIMIT_BYTES


# A Complete Job whose output cannot be read is a failed run, never an empty
# success.


def refuse_run(cluster: FakeCluster, code: str) -> None:
    with pytest.raises(ClusterError) as exc:
        run(cluster)
    assert str(exc.value).startswith(f"{code}: "), str(exc.value)
    assert_run_job_deleted(cluster)


def test_a_pod_list_error_after_completion_fails_the_run() -> None:
    cluster = FakeCluster()
    cluster.job_script = [COMPLETE]
    cluster.pod_list_answer = (500, {"kind": "Status", "status": "Failure", "code": 500})

    refuse_run(cluster, REFUSAL_RUN_FAILED)


def test_no_pods_after_completion_fails_the_run() -> None:
    cluster = FakeCluster()
    cluster.job_script = [COMPLETE]
    cluster.pod_list_answer = (200, {"kind": "PodList", "items": []})

    refuse_run(cluster, REFUSAL_RUN_FAILED)


def test_a_run_container_with_no_terminated_state_fails_the_run() -> None:
    cluster = FakeCluster()
    cluster.job_script = [COMPLETE]
    cluster.pod_script = [RUNNING]

    refuse_run(cluster, REFUSAL_RUN_FAILED)


@pytest.mark.parametrize("status", [404, 500])
def test_an_unreadable_run_log_fails_the_run(status: int) -> None:
    cluster = FakeCluster()
    cluster.log_answer = (status, "unavailable")

    refuse_run(cluster, REFUSAL_RUN_FAILED)


# activeDeadlineSeconds ends a Job with condition Failed, reason
# DeadlineExceeded, and terminates its running pods, so the run container
# reports exitCode 137 (SIGKILL)
# (https://kubernetes.io/docs/concepts/workloads/controllers/job/#job-termination-and-cleanup).
DEADLINE_EXCEEDED: dict[str, Any] = {
    "failed": 1,
    "conditions": [
        condition(
            "Failed",
            reason="DeadlineExceeded",
            message="Job was active longer than specified deadline",
        )
    ],
}


def test_a_deadline_exceeded_job_with_a_killed_container_times_out() -> None:
    cluster = FakeCluster()
    cluster.job_script = [ACTIVE, DEADLINE_EXCEEDED]
    cluster.pod_script = [RUNNING, terminated(137)]

    refuse_run(cluster, REFUSAL_RUN_TIMEOUT)


# AC2: logs


def app_pod(name: str, labels: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": NS, "labels": labels or {"app": "web"}},
        "spec": {"containers": [container("web"), container("sidecar")]},
    }


def test_logs_reads_the_named_container_with_a_tail() -> None:
    cluster = FakeCluster()
    cluster.existing[f"{CORE}/pods/web-5d9c"] = app_pod("web-5d9c")
    cluster.logs["web-5d9c"] = "listening on :8080\n"

    result = read_logs(cluster, install(), caller(), "web-5d9c", "sidecar", 50)

    assert result == {"logs": "listening on :8080\n"}
    [path] = cluster.text_calls
    parsed = urllib.parse.urlsplit(path)
    assert parsed.path == f"{CORE}/pods/web-5d9c/log"
    query = dict(urllib.parse.parse_qsl(parsed.query))
    assert query["container"] == "sidecar"
    assert query["tailLines"] == "50"
    assert cluster.writes() == []


def test_logs_for_a_missing_pod_is_refused() -> None:
    cluster = FakeCluster()

    with pytest.raises(ClusterError) as exc:
        read_logs(cluster, install(), caller(), "gone-1234", None, None)

    assert refusal(exc)[0] == REFUSAL_POD_NOT_FOUND
    assert cluster.text_calls == []


def test_logs_of_a_build_pod_are_refused_without_reading_them() -> None:
    cluster = FakeCluster()
    name = "e2e-build-0a1b2c3d-x7k2p"
    cluster.existing[f"{CORE}/pods/{name}"] = app_pod(name, {BUILD_LABEL: "e2e-build-0a1b2c3d"})
    cluster.logs[name] = "push credentials would be here"

    with pytest.raises(ClusterError) as exc:
        read_logs(cluster, install(), caller(), name, None, None)

    assert refusal(exc)[0] == REFUSAL_LOGS_REFUSED
    assert cluster.text_calls == []


def test_an_invalid_pod_name_is_refused_before_any_call() -> None:
    cluster = FakeCluster()

    with pytest.raises(ClusterError):
        read_logs(cluster, install(), caller(), "../x", None, None)

    assert cluster.calls == []
    assert cluster.text_calls == []


# AC2: events


def event(
    name: str,
    reason: str,
    stamp: str,
    *,
    count: int | None = 1,
    series: int | None = None,
    kind: str = "Normal",
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": f"{name}.{reason.lower()}", "namespace": NS},
        "involvedObject": {"kind": "Pod", "name": name, "namespace": NS},
        "reason": reason,
        "message": f"{reason} for {name}",
        "type": kind,
        "lastTimestamp": stamp,
        "count": count,
    }
    if series is not None:
        item["series"] = {"count": series, "lastObservedTime": stamp}
    return item


def test_events_report_reason_message_type_and_count() -> None:
    cluster = FakeCluster()
    cluster.events = [
        event("web-5d9c", "Scheduled", "2026-10-03T12:00:00Z"),
        event("web-5d9c", "BackOff", "2026-10-03T12:00:10Z", count=None, series=4, kind="Warning"),
        event("web-5d9c", "Pulled", "2026-10-03T12:00:20Z", count=None),
    ]

    result = list_events(cluster, install(), caller(), None)

    assert result == {
        "events": [
            {
                "reason": "Scheduled",
                "message": "Scheduled for web-5d9c",
                "type": "Normal",
                "count": 1,
            },
            {"reason": "BackOff", "message": "BackOff for web-5d9c", "type": "Warning", "count": 4},
            {"reason": "Pulled", "message": "Pulled for web-5d9c", "type": "Normal", "count": 1},
        ]
    }
    assert cluster.writes() == []


def test_events_filter_by_the_involved_object() -> None:
    cluster = FakeCluster()
    cluster.events = [
        event("web-5d9c", "Scheduled", "2026-10-03T12:00:00Z"),
        event("db-0", "Scheduled", "2026-10-03T12:00:01Z"),
    ]

    result = list_events(cluster, install(), caller(), "db-0")

    assert [e["message"] for e in result["events"]] == ["Scheduled for db-0"]
    [path] = [p for m, p, _b in cluster.calls if urllib.parse.urlsplit(p).path == f"{CORE}/events"]
    query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(path).query))
    assert query["fieldSelector"] == "involvedObject.name=db-0"


# AC3/AC4: cluster scoped objects


def test_a_crd_beside_a_deployment_is_refused_whole() -> None:
    cluster = FakeCluster()

    exc = refuse_deploy(cluster, manifest(crd(), deployment()), REFUSAL_CLUSTER_SCOPED)

    assert refusal_json(exc) == {
        "objects": [
            {
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "name": "sandboxes.agents.example.com",
            }
        ]
    }


@pytest.mark.parametrize(
    ("api_version", "kind"),
    [
        ("scheduling.k8s.io/v1", "PriorityClass"),
        ("rbac.authorization.k8s.io/v1", "ClusterRole"),
        ("rbac.authorization.k8s.io/v1", "ClusterRoleBinding"),
        ("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration"),
        ("admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration"),
        ("v1", "Namespace"),
        ("apiextensions.k8s.io/v1", "CustomResourceDefinition"),
    ],
)
def test_each_cluster_scoped_kind_is_refused(api_version: str, kind: str) -> None:
    cluster = FakeCluster()

    exc = refuse_deploy(
        cluster, manifest(cluster_object(api_version, kind, "thing")), REFUSAL_CLUSTER_SCOPED
    )

    assert refusal_json(exc) == {
        "objects": [{"apiVersion": api_version, "kind": kind, "name": "thing"}]
    }


def test_a_controller_bundle_lists_every_cluster_scoped_object() -> None:
    cluster = FakeCluster()
    controller = deployment("sandbox-controller", namespace="agent-sandbox-system")
    bundle = manifest(
        crd(),
        cluster_object(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            "sandbox-controller",
            rules=[{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]}],
        ),
        cluster_object(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            "sandbox-controller",
            roleRef={
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "sandbox-controller",
            },
            subjects=[
                {
                    "kind": "ServiceAccount",
                    "name": "sandbox-controller",
                    "namespace": "agent-sandbox-system",
                }
            ],
        ),
        controller,
    )

    exc = refuse_deploy(cluster, bundle, REFUSAL_CLUSTER_SCOPED)

    assert sorted((o["kind"], o["name"]) for o in refusal_json(exc)["objects"]) == [
        ("ClusterRole", "sandbox-controller"),
        ("ClusterRoleBinding", "sandbox-controller"),
        ("CustomResourceDefinition", "sandboxes.agents.example.com"),
    ]


def test_a_cluster_scoped_custom_kind_is_refused_through_discovery() -> None:
    cluster = FakeCluster()

    exc = refuse_deploy(
        cluster, manifest(cluster_object("example.com/v1", "Gadget", "g1")), REFUSAL_CLUSTER_SCOPED
    )

    assert refusal_json(exc) == {
        "objects": [{"apiVersion": "example.com/v1", "kind": "Gadget", "name": "g1"}]
    }


def test_cluster_scope_is_reported_before_a_tagged_image() -> None:
    cluster = FakeCluster()
    refuse_deploy(cluster, manifest(crd(), deployment(image="nginx:1.27")), REFUSAL_CLUSTER_SCOPED)


def test_the_cluster_scoped_code_differs_from_tag_and_write_failures() -> None:
    with pytest.raises(ClusterError) as scoped:
        run_deploy(FakeCluster(), manifest(crd()))
    with pytest.raises(ClusterError) as tagged:
        run_deploy(FakeCluster(), manifest(deployment(image="nginx:1.27")))
    failing = FakeCluster()
    failing.post_answers[f"{APPS}/deployments"] = (
        422,
        {
            "kind": "Status",
            "status": "Failure",
            "reason": "Invalid",
            "code": 422,
            "message": 'Deployment.apps "web" is invalid: spec.replicas: Invalid value: -1',
        },
    )
    with pytest.raises(ClusterError) as write_failed:
        run_deploy(failing, manifest(deployment()))

    codes = [refusal(scoped)[0], refusal(tagged)[0], refusal(write_failed)[0]]
    assert codes == [
        contract.REFUSAL_CLUSTER_SCOPED,
        contract.REFUSAL_IMAGE_NOT_DIGEST,
        contract.REFUSAL_DEPLOY_FAILED,
    ]
    assert len(set(codes)) == 3
    assert REFUSAL_DEPLOY_FAILED == "e2e_deploy_failed"
    assert "web" in refusal(write_failed)[1]


# HttpxCluster.read_text


def test_httpx_cluster_reads_text_plain_bodies() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(
            200, text="line one\nline two\n", headers={"content-type": "text/plain"}
        )

    client = httpx.Client(base_url="https://k8.test", transport=httpx.MockTransport(handler))
    path = f"{CORE}/pods/web-5d9c/log?tailLines=10"

    assert HttpxCluster(client).read_text(path) == (200, "line one\nline two\n")
    assert seen == [f"https://k8.test{path}"]


def test_httpx_cluster_read_text_raises_on_transport_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    client = httpx.Client(base_url="https://k8.test", transport=httpx.MockTransport(handler))

    with pytest.raises(ClusterError):
        HttpxCluster(client).read_text(f"{CORE}/pods/web-5d9c/log")
