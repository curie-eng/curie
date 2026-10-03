"""image_build against a fake test cluster API (#3246, ADR 0176 decision 6).

The fake answers at the ``ClusterApi`` seam with Kubernetes shapes from the
API reference (https://kubernetes.io/docs/reference/kubernetes-api/):

* Job ``status.conditions`` (types Complete and Failed, plus SuccessCriteriaMet
  and FailureTarget, which the Job controller adds ahead of the terminal
  condition from Kubernetes 1.31), ``status.succeeded`` and ``status.failed``
  (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/job-v1/#JobStatus).
* Pod ``status.initContainerStatuses`` and ``status.containerStatuses`` with
  ``state.terminated.{exitCode, reason, message}``
  (https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodStatus).
* ConfigMap ``metadata.resourceVersion``; a PUT with a stale one answers 409
  (https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions).
"""

from __future__ import annotations

import base64
import json
import urllib.parse
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from curie_e2e_connector.build import BuildConfig, BuildRequest, build_image
from curie_e2e_connector.contract import (
    BUILD_CACHE_K8S_SECRET_PREFIX,
    BUILD_EGRESS_POLICY,
    BUILD_LABEL,
    BUILD_PUSH_K8S_SECRET_PREFIX,
    DEFAULT_BUILDER_IMAGE,
    DEFAULT_GIT_IMAGE,
    DEFAULT_PUSH_IMAGE,
    IMAGES_CONFIGMAP,
    IMAGES_CONFIGMAP_KEY,
    OWNER_LABEL,
    POD_SECURITY_LABEL,
    PUSH_SCRIPT,
    RUN_LABEL,
    WORK_ITEM_LABEL,
)
from curie_e2e_connector.kube import ClusterApi, ClusterError
from curie_e2e_connector.namespace import Caller, Install, require_caller
from curie_e2e_connector.registry import RegistrySettings

RUN = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
OTHER_RUN = "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"
WORK = "11111111-2222-4333-8444-555555555555"
NS = f"curie-e2e-{RUN}"
HOST = "registry.test.example"
PREFIX = f"{HOST}/e2e"
REPO = f"{PREFIX}/{NS}/app"
CACHE_REPO = f"{HOST}/cache"
SOURCE = "https://github.com/example-org/example-app"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
BUILD_ID = "0a1b2c3d"
JOB = f"e2e-build-{BUILD_ID}"
DIGEST = "sha256:" + "ab" * 32
PUSH_SECRET = f"{BUILD_PUSH_K8S_SECRET_PREFIX}{BUILD_ID}"
CACHE_SECRET = f"{BUILD_CACHE_K8S_SECRET_PREFIX}{BUILD_ID}"

PUSH_PASSWORD = "push-pass-SENTINEL-41c"
PUSH_AUTH = base64.b64encode(f"pusher:{PUSH_PASSWORD}".encode()).decode()
CACHE_PASSWORD = "cache-pass-SENTINEL-82d"
PUSH_CONFIG = json.dumps({"auths": {HOST: {"auth": PUSH_AUTH}}})
CACHE_CONFIG = json.dumps({"auths": {HOST: {"username": "cacher", "password": CACHE_PASSWORD}}})

LEDGER_PATH = f"/api/v1/namespaces/{NS}/configmaps/{IMAGES_CONFIGMAP}"


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


def config(
    *,
    prefix: str = PREFIX,
    cache_repo: str = CACHE_REPO,
    insecure: bool = False,
    timeout_seconds: int = 600,
    token_hosts: tuple[str, ...] = (),
    source_hosts: tuple[str, ...] = ("github.com",),
) -> BuildConfig:
    return BuildConfig(
        registry=RegistrySettings(prefix=prefix, insecure=insecure, token_hosts=token_hosts),
        cache_repo=cache_repo,
        builder_image=DEFAULT_BUILDER_IMAGE,
        git_image=DEFAULT_GIT_IMAGE,
        push_image=DEFAULT_PUSH_IMAGE,
        timeout_seconds=timeout_seconds,
        source_hosts=source_hosts,
        poll_seconds=5,
    )


def caller() -> Caller:
    return require_caller(RUN, WORK)


def namespace_object(
    *, run: str = RUN, phase: str = "Active", pod_security: str = "baseline"
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": NS,
            "labels": {
                OWNER_LABEL: "acme",
                RUN_LABEL: run,
                WORK_ITEM_LABEL: WORK,
                POD_SECURITY_LABEL: pod_security,
            },
        },
        "status": {"phase": phase},
    }


def condition(kind: str, *, reason: str = "", message: str = "") -> dict[str, Any]:
    return {
        "type": kind,
        "status": "True",
        "reason": reason,
        "message": message,
        "lastTransitionTime": "2026-10-02T12:01:00Z",
    }


ACTIVE: dict[str, Any] = {"active": 1, "ready": 1}
SUCCEEDED: dict[str, Any] = {
    "succeeded": 1,
    # Kubernetes 1.31+: SuccessCriteriaMet precedes Complete.
    "conditions": [
        condition("SuccessCriteriaMet", reason="CompletionsReached"),
        condition("Complete", reason="CompletionsReached"),
    ],
}


def failed(reason: str, message: str) -> dict[str, Any]:
    # Kubernetes 1.31+: FailureTarget precedes Failed.
    return {
        "failed": 1,
        "conditions": [
            condition("FailureTarget", reason=reason, message=message),
            condition("Failed", reason=reason, message=message),
        ],
    }


def terminated(name: str, code: int = 0, *, message: str = "", reason: str = "") -> dict[str, Any]:
    state: dict[str, Any] = {
        "exitCode": code,
        "reason": reason or ("Completed" if code == 0 else "Error"),
    }
    if message:
        state["message"] = message
    return {"name": name, "ready": False, "restartCount": 0, "state": {"terminated": state}}


def waiting(name: str) -> dict[str, Any]:
    return {"name": name, "ready": False, "state": {"waiting": {"reason": "PodInitializing"}}}


def success_pod(job: str, message: str) -> dict[str, Any]:
    return {
        "metadata": {"name": f"{job}-x7k2p", "labels": {"job-name": job, BUILD_LABEL: job}},
        "status": {
            "phase": "Succeeded",
            "initContainerStatuses": [terminated("source"), terminated("build")],
            "containerStatuses": [terminated("push", message=message)],
        },
    }


class FakeBuildCluster(ClusterApi):
    """Namespace, ledger ConfigMap, NetworkPolicy, Secrets and build Jobs for one run."""

    def __init__(
        self,
        *,
        namespace: dict[str, Any] | None = None,
        ledger: dict[str, Any] | None = None,
    ) -> None:
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.namespace = namespace if namespace is not None else namespace_object()
        self.ledger_text: str | None = None if ledger is None else json.dumps(ledger)
        self.resource_version = 3
        self.post_status: dict[str, int] = {}
        self.jobs: dict[str, dict[str, Any]] = {}
        self.job_scripts: dict[str, list[dict[str, Any]]] = {}
        self.default_script: list[dict[str, Any]] = [ACTIVE, SUCCEEDED]
        self.job_gets: dict[str, int] = {}
        self.pods: dict[str, list[dict[str, Any]]] = {}
        self.push_messages: dict[str, str] = {}
        self.put_hook: Callable[[FakeBuildCluster], int | None] | None = None
        self.after_job_post: Callable[[FakeBuildCluster], None] | None = None
        self.job_get_error: Callable[[int], int | None] | None = None
        self.pod_selectors: list[str] = []
        # Secrets the API server holds, by name. A POST that answers 201 stores one.
        self.secrets: dict[str, dict[str, Any]] = {}
        # Secret names whose POST persists the object and then raises a transport error.
        self.post_transport_error: set[str] = set()

    # Ledger helpers

    def ledger(self) -> dict[str, Any] | None:
        return None if self.ledger_text is None else json.loads(self.ledger_text)

    def configmap(self) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": IMAGES_CONFIGMAP,
                "namespace": NS,
                "resourceVersion": str(self.resource_version),
            },
            "data": {IMAGES_CONFIGMAP_KEY: self.ledger_text},
        }

    def close(self) -> None:
        """What teardown does: stamp closing_at and bump the resourceVersion."""

        current = self.ledger() or {"repositories": []}
        current["closing_at"] = "2026-10-02T12:00:00Z"
        self.ledger_text = json.dumps(current)
        self.resource_version += 1

    # Call log views

    def writes(self) -> list[tuple[str, str, str]]:
        """(method, kind, name) for every POST and PUT, in order."""

        out = []
        for method, _path, body in self.calls:
            if method in ("POST", "PUT") and body is not None:
                out.append((method, str(body.get("kind")), str(body["metadata"]["name"])))
        return out

    def posted(self, kind: str) -> list[dict[str, Any]]:
        return [
            body
            for method, _path, body in self.calls
            if method == "POST" and body is not None and body.get("kind") == kind
        ]

    def job_body(self) -> dict[str, Any]:
        [job] = self.posted("Job")
        return job

    def deleted(self, collection: str) -> list[str]:
        names = []
        for method, path, _body in self.calls:
            bare = urllib.parse.urlsplit(path).path
            if method == "DELETE" and f"/{collection}/" in bare:
                names.append(bare.rsplit("/", 1)[-1])
        return names

    def created_secrets(self) -> list[str]:
        return [
            str(body["metadata"]["name"])
            for body in self.posted("Secret")
            if self.post_status.get(str(body["metadata"]["name"]), 201) == 201
        ]

    def job_deletes(self) -> list[tuple[str, str, dict[str, Any] | None]]:
        return [
            call
            for call in self.calls
            if call[0] == "DELETE" and "/jobs/" in urllib.parse.urlsplit(call[1]).path
        ]

    # The API

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        parsed = urllib.parse.urlsplit(path)
        bare = parsed.path
        query = dict(urllib.parse.parse_qsl(parsed.query))
        if len(self.calls) > 2000:
            raise AssertionError("the build never stopped polling")

        if bare == f"/api/v1/namespaces/{NS}" and method == "GET":
            if self.namespace is None:
                return 404, {"kind": "Status", "reason": "NotFound", "code": 404}
            return 200, self.namespace

        if bare == LEDGER_PATH:
            if method == "GET":
                if self.ledger_text is None:
                    return 404, {"kind": "Status", "reason": "NotFound", "code": 404}
                return 200, self.configmap()
            if method == "PUT":
                assert body is not None
                if self.put_hook is not None:
                    forced = self.put_hook(self)
                    if forced is not None:
                        return forced, {"kind": "Status", "reason": "Conflict", "code": forced}
                if body["metadata"].get("resourceVersion") != str(self.resource_version):
                    return 409, {"kind": "Status", "reason": "Conflict", "code": 409}
                self.ledger_text = body["data"][IMAGES_CONFIGMAP_KEY]
                self.resource_version += 1
                return 200, self.configmap()

        if bare == f"/api/v1/namespaces/{NS}/configmaps" and method == "POST":
            assert body is not None
            if self.ledger_text is not None:
                return 409, {"kind": "Status", "reason": "AlreadyExists", "code": 409}
            self.ledger_text = body["data"][IMAGES_CONFIGMAP_KEY]
            return 201, self.configmap()

        secret_prefix = f"/api/v1/namespaces/{NS}/secrets/"
        if bare.startswith(secret_prefix) and method in ("GET", "DELETE"):
            name = bare.removeprefix(secret_prefix)
            if method == "GET":
                if name not in self.secrets:
                    return 404, {"kind": "Status", "reason": "NotFound", "code": 404}
                return 200, self.secrets[name]
            self.secrets.pop(name, None)
            return 200, {"kind": "Status", "status": "Success"}

        if method == "POST":
            assert body is not None
            name = str(body["metadata"]["name"])
            status = self.post_status.get(name, 201)
            if body.get("kind") == "Secret":
                if name in self.post_transport_error:
                    self.secrets.setdefault(name, body)
                    raise ClusterError("the test cluster API is unreachable")
                if status == 201:
                    self.secrets[name] = body
            if status == 201 and body.get("kind") == "Job":
                self.jobs[name] = body
                if self.after_job_post is not None:
                    self.after_job_post(self)
            return status, body if status == 201 else {"kind": "Status", "code": status}

        if bare.startswith(f"/apis/batch/v1/namespaces/{NS}/jobs/") and method == "GET":
            name = bare.rsplit("/", 1)[-1]
            count = self.job_gets.get(name, 0)
            self.job_gets[name] = count + 1
            if self.job_get_error is not None:
                forced = self.job_get_error(count)
                if forced is not None:
                    return forced, {"kind": "Status", "code": forced}
            script = self.job_scripts.get(name, self.default_script)
            status = script[min(count, len(script) - 1)]
            return 200, {"kind": "Job", "metadata": {"name": name}, "status": status}

        if bare == f"/api/v1/namespaces/{NS}/pods" and method == "GET":
            selector = query.get("labelSelector", "")
            self.pod_selectors.append(selector)
            job = selector.removeprefix("job-name=")
            if job in self.pods:
                return 200, {"kind": "PodList", "items": self.pods[job]}
            message = self.push_messages.get(job, f"{REPO}@{DIGEST}")
            return 200, {"kind": "PodList", "items": [success_pod(job, message)]}

        if method == "DELETE":
            return 200, {"kind": "Status", "status": "Success"}

        return 500, {"kind": "Status", "code": 500}


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def parse(**overrides: Any) -> BuildRequest:
    arguments: dict[str, Any] = {
        "context": "services/api",
        "dockerfile": "Dockerfile",
        "platforms": None,
        "repository": SOURCE,
        "commit": COMMIT,
        "name": "app",
    }
    arguments.update(overrides)
    return BuildRequest.parse(**arguments)


def run_build(
    cluster: FakeBuildCluster,
    *,
    cfg: BuildConfig | None = None,
    inst: Install | None = None,
    who: Caller | None = None,
    push: str | None = PUSH_CONFIG,
    cache: str | None = CACHE_CONFIG,
    build_id: str = BUILD_ID,
    polls: list[float] | None = None,
    clock: FakeClock | None = None,
    **arguments: Any,
) -> dict[str, Any]:
    """Parse the tool arguments and build, as the tool does."""

    tick = clock or FakeClock()
    return build_image(
        cluster,
        inst or install(),
        cfg or config(),
        who or caller(),
        parse(**arguments),
        push_config_text=push,
        cache_config_text=cache,
        clock=tick.clock,
        sleep=tick.sleep,
        build_id=build_id,
        on_poll=(polls if polls is not None else []).append,
    )


def pod_spec(cluster: FakeBuildCluster) -> dict[str, Any]:
    return cluster.job_body()["spec"]["template"]["spec"]


def containers(cluster: FakeBuildCluster) -> dict[str, dict[str, Any]]:
    spec = pod_spec(cluster)
    return {c["name"]: c for c in [*spec.get("initContainers", []), *spec["containers"]]}


def secret_mounts(cluster: FakeBuildCluster) -> dict[str, set[str]]:
    """Container name to the Secret names its volume mounts reach."""

    spec = pod_spec(cluster)
    by_volume: dict[str, set[str]] = {}
    for volume in spec.get("volumes", []):
        names: set[str] = set()
        if "secret" in volume:
            names.add(volume["secret"]["secretName"])
        for source in volume.get("projected", {}).get("sources", []):
            if "secret" in source:
                names.add(source["secret"]["name"])
        by_volume[volume["name"]] = names
    return {
        name: set().union(*[by_volume.get(m["name"], set()) for m in c.get("volumeMounts", [])])
        for name, c in containers(cluster).items()
    }


def env_of(container: dict[str, Any]) -> dict[str, str]:
    return {item["name"]: item.get("value", "") for item in container.get("env", [])}


def no_writes(cluster: FakeBuildCluster) -> bool:
    return not any(method in ("POST", "PUT", "DELETE") for method, _p, _b in cluster.calls)


# Refusals


def test_a_caller_without_a_run_is_refused_by_the_tool() -> None:
    from curie_e2e_connector import server

    with pytest.raises(Exception, match="e2e_run_identity_required"):
        server.image_build(
            SimpleNamespace(headers={}), context=".", repository=SOURCE, commit=COMMIT
        )


def test_a_namespace_owned_by_another_run_is_refused_without_writes() -> None:
    cluster = FakeBuildCluster(namespace=namespace_object(run=OTHER_RUN))
    with pytest.raises(ClusterError, match="e2e_namespace_not_owned"):
        run_build(cluster)
    assert no_writes(cluster)


@pytest.mark.parametrize(
    "namespace",
    [None, namespace_object(phase="Terminating")],
    ids=["missing", "terminating"],
)
def test_no_live_environment_is_refused_without_writes(namespace: dict[str, Any] | None) -> None:
    cluster = FakeBuildCluster()
    cluster.namespace = namespace  # type: ignore[assignment]
    with pytest.raises(ClusterError, match="e2e_environment_required"):
        run_build(cluster)
    assert no_writes(cluster)


ARGUMENT_REFUSALS = [
    pytest.param("commit", {"commit": "0123abc"}, id="commit-short"),
    pytest.param("commit", {"commit": COMMIT.upper()}, id="commit-uppercase"),
    pytest.param("commit", {"commit": COMMIT[:39]}, id="commit-39-hex"),
    pytest.param("commit", {"commit": "main"}, id="commit-branch-name"),
    pytest.param("repository", {"repository": "http://github.com/example-org/x"}, id="repo-http"),
    pytest.param("repository", {"repository": "https://gitlab.com/example-org/x"}, id="repo-host"),
    pytest.param(
        "repository", {"repository": "https://user:pw@github.com/example-org/x"}, id="repo-userinfo"
    ),
    pytest.param("repository", {"repository": f"{SOURCE}?ref=main"}, id="repo-query"),
    pytest.param("repository", {"repository": f"{SOURCE}#frag"}, id="repo-fragment"),
    pytest.param("repository", {"repository": "github.com/example-org/x"}, id="repo-no-scheme"),
    pytest.param("context", {"context": "../x"}, id="context-parent"),
    pytest.param("context", {"context": "/abs"}, id="context-absolute"),
    pytest.param("context", {"context": "a/../b"}, id="context-inner-parent"),
    pytest.param("context", {"context": "a\\b"}, id="context-backslash"),
    pytest.param("context", {"context": "a//b"}, id="context-empty-segment"),
    pytest.param("context", {"context": "a" * 257}, id="context-too-long"),
    pytest.param("dockerfile", {"dockerfile": "../Dockerfile"}, id="dockerfile-parent"),
    pytest.param("dockerfile", {"dockerfile": "/Dockerfile"}, id="dockerfile-absolute"),
    pytest.param("platforms", {"platforms": ["linux/amd64", "linux/arm64"]}, id="platforms-two"),
    pytest.param("platforms", {"platforms": ["Linux/AMD64"]}, id="platforms-bad"),
    pytest.param("name", {"name": "App"}, id="name-uppercase"),
    pytest.param("name", {"name": "a/b"}, id="name-slash"),
    pytest.param("name", {"name": "-app"}, id="name-leading-dash"),
    pytest.param("name", {"name": "a" * 64}, id="name-too-long"),
]


@pytest.mark.parametrize(("field", "arguments"), ARGUMENT_REFUSALS)
def test_argument_refusals_name_the_field_and_send_nothing(
    field: str, arguments: dict[str, Any]
) -> None:
    cluster = FakeBuildCluster()
    with pytest.raises(ClusterError, match="e2e_build_argument_refused") as excinfo:
        run_build(cluster, **arguments)
    assert field in str(excinfo.value)
    assert cluster.calls == []


@pytest.mark.parametrize(
    "arguments",
    [
        {"context": "."},
        {"context": "services/api", "dockerfile": "docker/Dockerfile.prod"},
        {"platforms": ["linux/arm64"]},
        {"platforms": ["linux/arm/v7"]},
        {"platforms": []},
        {"name": "web-2"},
        {"repository": "https://github.com/example-org/example-app.git"},
    ],
    ids=[
        "context-dot",
        "nested-dockerfile",
        "one-platform",
        "variant",
        "no-platforms",
        "name",
        "dot-git",
    ],
)
def test_valid_arguments_are_not_refused(arguments: dict[str, Any]) -> None:
    cluster = FakeBuildCluster()
    name = arguments.get("name", "app")
    cluster.push_messages[JOB] = f"{PREFIX}/{NS}/{name}@{DIGEST}"
    result = run_build(cluster, **arguments)
    assert result["images"][0]["digest"] == DIGEST


def test_restricted_install_is_refused_before_any_call() -> None:
    cluster = FakeBuildCluster()
    with pytest.raises(ClusterError, match="e2e_build_needs_baseline"):
        run_build(cluster, inst=install(pod_security="restricted"))
    assert cluster.calls == []


def test_a_restricted_namespace_is_refused_without_writes() -> None:
    cluster = FakeBuildCluster(namespace=namespace_object(pod_security="restricted"))
    with pytest.raises(ClusterError, match="e2e_build_needs_baseline"):
        run_build(cluster)
    assert no_writes(cluster)


def test_an_unconfigured_registry_is_refused_before_any_call() -> None:
    cluster = FakeBuildCluster()
    with pytest.raises(ClusterError, match="e2e_registry_not_configured"):
        run_build(cluster, cfg=config(prefix=""))
    assert cluster.calls == []


@pytest.mark.parametrize(
    "who",
    [Caller('aaaa"bbbb', WORK), Caller("$(id)", WORK), Caller(RUN, "x;$HOME")],
    ids=["quote-run", "dollar-run", "dollar-work-item"],
)
def test_a_label_value_with_shell_characters_is_refused_before_any_call(who: Caller) -> None:
    cluster = FakeBuildCluster()
    with pytest.raises(ClusterError):
        run_build(cluster, who=who)
    assert cluster.calls == []


@pytest.mark.parametrize("owner", ['ac"me', "ac$me"])
def test_an_owner_label_with_shell_characters_is_refused_before_any_call(owner: str) -> None:
    cluster = FakeBuildCluster()
    with pytest.raises(ClusterError):
        run_build(cluster, inst=install(owner_label_value=owner))
    assert cluster.calls == []


def test_build_config_validate() -> None:
    config().validate()
    config(prefix="", cache_repo="").validate()
    for bad in (
        config(timeout_seconds=30),
        config(timeout_seconds=3601),
        config(prefix="Registry.Example/E2E"),
        config(cache_repo="registry.test.example/Cache/../x"),
        config(token_hosts=("https://auth.test.example/token",)),
    ):
        with pytest.raises(ClusterError, match="e2e_connector_misconfigured"):
            bad.validate()


def test_build_config_defaults() -> None:
    cfg = BuildConfig(
        registry=RegistrySettings(prefix=PREFIX, insecure=False, token_hosts=()),
        cache_repo="",
        builder_image=DEFAULT_BUILDER_IMAGE,
        git_image=DEFAULT_GIT_IMAGE,
        push_image=DEFAULT_PUSH_IMAGE,
    )
    assert cfg.timeout_seconds == 1200
    assert tuple(cfg.source_hosts) == ("github.com",)
    assert cfg.poll_seconds == 5


# Liveness


def test_a_build_writes_in_order_and_returns_the_pushed_digest() -> None:
    cluster = FakeBuildCluster()

    result = run_build(cluster)

    assert result == {"images": [{"name": REPO, "digest": DIGEST}]}
    kinds = [(method, kind) for method, kind, _name in cluster.writes()]
    assert kinds == [
        ("POST", "ConfigMap"),
        ("POST", "NetworkPolicy"),
        ("POST", "Secret"),
        ("POST", "Secret"),
        ("POST", "Job"),
    ]
    assert [name for _m, _k, name in cluster.writes()] == [
        IMAGES_CONFIGMAP,
        BUILD_EGRESS_POLICY,
        PUSH_SECRET,
        CACHE_SECRET,
        JOB,
    ]
    assert cluster.pod_selectors == [f"job-name={JOB}"]


def test_the_job_spec() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, cfg=config(timeout_seconds=900))
    job = cluster.job_body()
    assert job["metadata"]["name"] == JOB
    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == 900
    assert job["spec"]["ttlSecondsAfterFinished"] == 600
    expected = {
        OWNER_LABEL: "acme",
        RUN_LABEL: RUN,
        WORK_ITEM_LABEL: WORK,
        BUILD_LABEL: JOB,
    }
    assert expected.items() <= job["metadata"]["labels"].items()
    assert expected.items() <= job["spec"]["template"]["metadata"]["labels"].items()
    spec = pod_spec(cluster)
    assert spec["restartPolicy"] == "Never"
    assert spec["automountServiceAccountToken"] is False
    assert spec["enableServiceLinks"] is False


def test_containers_are_unprivileged_with_no_host_access() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    spec = pod_spec(cluster)
    assert [c["name"] for c in spec["initContainers"]] == ["source", "build"]
    assert [c["name"] for c in spec["containers"]] == ["push"]
    for flag in ("hostNetwork", "hostPID", "hostIPC"):
        assert spec.get(flag) in (None, False)
    for volume in spec.get("volumes", []):
        assert "hostPath" not in volume
    for name, container in containers(cluster).items():
        context = container.get("securityContext", {})
        assert context.get("privileged") is False, name
        assert container.get("terminationMessagePolicy") == "FallbackToLogsOnError", name
    push = containers(cluster)["push"]
    assert push["securityContext"]["allowPrivilegeEscalation"] is False
    assert push["securityContext"]["capabilities"]["drop"] == ["ALL"]


def test_the_source_container_fetches_the_commit_by_sha_without_credentials() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    source = containers(cluster)["source"]
    assert source["image"] == DEFAULT_GIT_IMAGE
    assert source["command"] == [
        "sh",
        "-ec",
        'git init -q /workspace && cd /workspace && git fetch -q --depth 1 "$1" "$2" '
        "&& git checkout -q FETCH_HEAD",
        "fetch",
        SOURCE,
        COMMIT,
    ]
    assert env_of(source).get("GIT_TERMINAL_PROMPT") == "0"
    assert secret_mounts(cluster)["source"] == set()


def test_secrets_are_dockerconfigjson_labelled_and_the_job_carries_no_credential() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    secrets = cluster.posted("Secret")
    assert [s["metadata"]["name"] for s in secrets] == [PUSH_SECRET, CACHE_SECRET]
    for secret in secrets:
        assert secret["type"] == "kubernetes.io/dockerconfigjson"
        labels = secret["metadata"]["labels"]
        assert labels[RUN_LABEL] == RUN
        assert labels[WORK_ITEM_LABEL] == WORK
        assert labels[OWNER_LABEL] == "acme"
        assert BUILD_LABEL in labels
    text = json.dumps(cluster.job_body())
    for credential in (PUSH_PASSWORD, PUSH_AUTH, CACHE_PASSWORD):
        assert credential not in text
    ledger_text = cluster.ledger_text or ""
    for credential in (PUSH_PASSWORD, PUSH_AUTH, CACHE_PASSWORD):
        assert credential not in ledger_text


def test_the_egress_policy_selects_build_pods() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    [policy] = cluster.posted("NetworkPolicy")
    assert policy["metadata"]["name"] == BUILD_EGRESS_POLICY
    selector = policy["spec"]["podSelector"]
    assert {"key": BUILD_LABEL, "operator": "Exists"} in selector["matchExpressions"]
    assert "Egress" in policy["spec"]["policyTypes"]
    ports = {
        (port.get("protocol", "TCP"), port["port"])
        for rule in policy["spec"]["egress"]
        for port in rule.get("ports", [])
    }
    assert {("TCP", 443), ("UDP", 53), ("TCP", 53)} <= ports


def _egress_ports(cluster: FakeBuildCluster) -> list[tuple[str, int]]:
    [policy] = cluster.posted("NetworkPolicy")
    return [
        (port.get("protocol", "TCP"), port["port"])
        for rule in policy["spec"]["egress"]
        for port in rule.get("ports", [])
    ]


def test_the_egress_policy_opens_a_source_host_port() -> None:
    cluster = FakeBuildCluster()
    hosts = ("github.com", "git.example.com:8443")
    run_build(
        cluster,
        cfg=config(source_hosts=hosts),
        repository="https://git.example.com:8443/org/app",
        source_hosts=hosts,
    )
    ports = set(_egress_ports(cluster))
    assert ("TCP", 8443) in ports
    assert {("TCP", 443), ("UDP", 53), ("TCP", 53)} <= ports


def test_the_egress_policy_opens_a_registry_token_host_port() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, cfg=config(token_hosts=("auth.example.com:8443",)))
    ports = set(_egress_ports(cluster))
    assert ("TCP", 8443) in ports
    assert {("TCP", 443), ("UDP", 53), ("TCP", 53)} <= ports


def test_the_egress_policy_deduplicates_extra_ports() -> None:
    cluster = FakeBuildCluster()
    hosts = ("github.com", "git.example.com:8443")
    run_build(
        cluster,
        cfg=config(token_hosts=("auth.example.com:8443",), source_hosts=hosts),
        repository="https://git.example.com:8443/org/app",
        source_hosts=hosts,
    )
    ports = _egress_ports(cluster)
    assert len(ports) == len(set(ports))
    assert ("TCP", 8443) in ports


def test_hosts_without_a_port_add_no_egress_port() -> None:
    baseline = FakeBuildCluster()
    run_build(baseline)
    cluster = FakeBuildCluster()
    run_build(cluster, cfg=config(token_hosts=("auth.example.com",)))
    assert _egress_ports(cluster) == _egress_ports(baseline)
    assert cluster.posted("NetworkPolicy") == baseline.posted("NetworkPolicy")


# Mount separation


def test_without_a_cache_config_build_mounts_no_secret() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, cache=None)
    mounts = secret_mounts(cluster)
    assert mounts["build"] == set()
    assert mounts["source"] == set()
    assert mounts["push"] == {PUSH_SECRET}
    assert [s["metadata"]["name"] for s in cluster.posted("Secret")] == [PUSH_SECRET]


def test_with_both_configs_build_mounts_only_the_cache_secret() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    mounts = secret_mounts(cluster)
    assert mounts["build"] == {CACHE_SECRET}
    assert mounts["source"] == set()
    assert mounts["push"] == {PUSH_SECRET}
    build = containers(cluster)["build"]
    cache_mount = [
        m["mountPath"] for m in build["volumeMounts"] if m["name"] not in ("workspace", "out")
    ]
    assert cache_mount == ["/kaniko/.docker"]
    push = containers(cluster)["push"]
    assert env_of(push)["DOCKER_CONFIG"] == "/docker-config"
    push_config_mount = [m for m in push["volumeMounts"] if m["mountPath"] == "/docker-config"]
    assert len(push_config_mount) == 1 and push_config_mount[0].get("readOnly") is True


def test_without_a_push_config_nothing_mounts_a_push_secret() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, push=None, cache=None)
    assert cluster.posted("Secret") == []
    assert all(names == set() for names in secret_mounts(cluster).values())
    assert "DOCKER_CONFIG" not in env_of(containers(cluster)["push"])


def test_the_push_container_mounts_the_tarball_read_only() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    push = containers(cluster)["push"]
    out = [m for m in push["volumeMounts"] if m["name"] == "out"]
    assert len(out) == 1 and out[0].get("readOnly") is True


# Cache rule


def build_args(cluster: FakeBuildCluster) -> list[str]:
    return list(containers(cluster)["build"]["args"])


def test_push_only_install_gets_no_cache() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, cache=None)
    args = build_args(cluster)
    assert "--cache=false" in args
    assert "--cache=true" not in args
    assert not any(arg.startswith("--cache-repo") for arg in args)
    assert not any(
        s["metadata"]["name"].startswith(BUILD_CACHE_K8S_SECRET_PREFIX)
        for s in cluster.posted("Secret")
    )


def test_an_empty_cache_repo_gets_no_cache_even_with_the_config() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, cfg=config(cache_repo=""))
    args = build_args(cluster)
    assert "--cache=false" in args
    assert not any(arg.startswith("--cache-repo") for arg in args)
    assert [s["metadata"]["name"] for s in cluster.posted("Secret")] == [PUSH_SECRET]


def test_cache_config_and_repo_enable_the_cache() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    args = build_args(cluster)
    assert "--cache=true" in args
    assert f"--cache-repo={CACHE_REPO}" in args
    assert "--cache=false" not in args


def test_build_args() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    build = containers(cluster)["build"]
    assert build["image"] == DEFAULT_BUILDER_IMAGE
    args = build_args(cluster)
    assert "--context=dir:///workspace/services/api" in args
    assert "--dockerfile=/workspace/services/api/Dockerfile" in args
    assert f"--destination={REPO}:build-{BUILD_ID}" in args
    assert "--no-push" in args
    assert "--tar-path=/out/image.tar" in args
    assert not any(arg.startswith("--insecure") for arg in args)


def test_a_dot_context_builds_the_workspace_root() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, context=".")
    args = build_args(cluster)
    assert "--context=dir:///workspace" in args
    assert any(
        arg in ("--dockerfile=/workspace/Dockerfile", "--dockerfile=/workspace/./Dockerfile")
        for arg in args
    )


# Insecure


INSECURE_PREFIX = f"{HOST}:5000/e2e"
INSECURE_REPO = f"{INSECURE_PREFIX}/{NS}/app"


def test_insecure_registry_flags_are_per_host() -> None:
    cluster = FakeBuildCluster()
    cluster.push_messages[JOB] = f"{INSECURE_REPO}@{DIGEST}"
    result = run_build(
        cluster,
        cfg=config(prefix=INSECURE_PREFIX, cache_repo="cache.test.example/cache", insecure=True),
    )
    assert result["images"] == [{"name": INSECURE_REPO, "digest": DIGEST}]
    args = build_args(cluster)
    assert f"--insecure-registry={HOST}:5000" in args
    assert "--insecure-registry=cache.test.example" in args
    assert "--insecure" not in args
    assert not any(arg.startswith("--insecure-pull") for arg in args)
    assert not any(arg.startswith("--insecure=") for arg in args)
    assert env_of(containers(cluster)["push"])["CRANE_INSECURE"] == "1"
    [policy] = cluster.posted("NetworkPolicy")
    ports = {port["port"] for rule in policy["spec"]["egress"] for port in rule.get("ports", [])}
    assert 5000 in ports


def test_insecure_with_one_host_adds_one_flag() -> None:
    cluster = FakeBuildCluster()
    cluster.push_messages[JOB] = f"{INSECURE_REPO}@{DIGEST}"
    run_build(
        cluster,
        cfg=config(prefix=INSECURE_PREFIX, cache_repo=f"{HOST}:5000/cache", insecure=True),
    )
    flags = [arg for arg in build_args(cluster) if arg.startswith("--insecure-registry")]
    assert flags == [f"--insecure-registry={HOST}:5000"]


def test_a_secure_registry_sets_no_crane_insecure() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    assert "CRANE_INSECURE" not in env_of(containers(cluster)["push"])


# Labels and the fixed push script


def test_kaniko_applies_no_labels() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    assert not any(arg.startswith("--label") for arg in build_args(cluster))
    build_text = json.dumps(containers(cluster)["build"])
    assert WORK not in build_text
    assert "acme" not in build_text


def test_push_runs_the_fixed_script_with_values_only_in_env() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    push = containers(cluster)["push"]
    assert push["image"] == DEFAULT_PUSH_IMAGE
    assert push["command"] == ["sh", "-c", PUSH_SCRIPT]
    assert push.get("args") in (None, [])
    env = env_of(push)
    assert env["DEST"] == REPO
    assert env["STAGING_TAG"] == f"staging-{BUILD_ID}"
    assert env["BUILD_TAG"] == f"build-{BUILD_ID}"
    assert env["LABEL_OWNER"] == "acme"
    assert env["LABEL_RUN"] == RUN
    assert env["LABEL_WORK_ITEM"] == WORK
    for value in (RUN, WORK, "acme", REPO, NS, COMMIT, SOURCE, BUILD_ID):
        assert value not in PUSH_SCRIPT, value
    # The source container sees the source; nothing else carries run identity.
    assert WORK not in json.dumps(containers(cluster)["source"])


def test_the_push_script_deletes_nothing_and_labels_by_env() -> None:
    assert "delete" not in PUSH_SCRIPT.lower()
    assert 'crane push ${CRANE_INSECURE:+--insecure} /out/image.tar "$DEST:$STAGING_TAG"' in (
        PUSH_SCRIPT
    )
    assert '--label "curietech.ai/e2e-owner=$LABEL_OWNER"' in PUSH_SCRIPT
    assert '--label "curietech.ai/e2e-run=$LABEL_RUN"' in PUSH_SCRIPT
    assert '--label "curietech.ai/e2e-work-item=$LABEL_WORK_ITEM"' in PUSH_SCRIPT
    assert '-t "$DEST:$BUILD_TAG"' in PUSH_SCRIPT
    assert "/dev/termination-log" in PUSH_SCRIPT
    assert PUSH_SCRIPT.lstrip().startswith("set -eu")


# Tags


def test_two_builds_of_one_commit_get_unique_tags_and_share_nothing_destructive() -> None:
    cluster = FakeBuildCluster()
    second_job = "e2e-build-0a1b2c3e"
    first_digest = "sha256:" + "11" * 32
    second_digest = "sha256:" + "22" * 32
    cluster.push_messages[JOB] = f"{REPO}@{first_digest}"
    cluster.push_messages[second_job] = f"{REPO}@{second_digest}"

    first = run_build(cluster, build_id="0a1b2c3d")
    second = run_build(cluster, build_id="0a1b2c3e")

    assert first == {"images": [{"name": REPO, "digest": first_digest}]}
    assert second == {"images": [{"name": REPO, "digest": second_digest}]}
    jobs = cluster.posted("Job")
    envs = [env_of(job["spec"]["template"]["spec"]["containers"][0]) for job in jobs]
    assert [env["BUILD_TAG"] for env in envs] == ["build-0a1b2c3d", "build-0a1b2c3e"]
    assert [env["STAGING_TAG"] for env in envs] == ["staging-0a1b2c3d", "staging-0a1b2c3e"]
    for env in envs:
        assert COMMIT not in (env["BUILD_TAG"], env["STAGING_TAG"])
        assert COMMIT[:12] not in env["BUILD_TAG"]
    # The only deletes are each build's own Secrets: no Job, manifest or tag delete.
    deletes = [path for method, path, _b in cluster.calls if method == "DELETE"]
    assert all("/secrets/" in path for path in deletes)
    assert sorted(cluster.deleted("secrets")) == sorted(
        [
            PUSH_SECRET,
            CACHE_SECRET,
            f"{BUILD_PUSH_K8S_SECRET_PREFIX}0a1b2c3e",
            f"{BUILD_CACHE_K8S_SECRET_PREFIX}0a1b2c3e",
        ]
    )


# Ledger first and admission


def test_the_ledger_is_written_before_any_secret_or_job() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    writes = cluster.writes()
    ledger_index = next(i for i, w in enumerate(writes) if w[1] == "ConfigMap")
    first_secret_or_job = next(i for i, w in enumerate(writes) if w[1] in ("Secret", "Job"))
    assert ledger_index < first_secret_or_job
    [configmap] = cluster.posted("ConfigMap")
    labels = configmap["metadata"]["labels"]
    assert labels[RUN_LABEL] == RUN and labels[WORK_ITEM_LABEL] == WORK
    assert labels[OWNER_LABEL] == "acme"
    assert cluster.ledger() == {"repositories": [REPO]}


def test_the_ledger_never_holds_a_digest() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster)
    assert "sha256:" not in (cluster.ledger_text or "")


def test_a_second_build_does_not_duplicate_and_a_new_name_appends() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, build_id="0a1b2c3d")
    run_build(cluster, build_id="0a1b2c3e")
    assert cluster.ledger() == {"repositories": [REPO]}
    cluster.push_messages["e2e-build-0a1b2c3f"] = f"{PREFIX}/{NS}/worker@{DIGEST}"
    run_build(cluster, build_id="0a1b2c3f", name="worker")
    assert cluster.ledger() == {"repositories": [REPO, f"{PREFIX}/{NS}/worker"]}
    puts = [body for method, _p, body in cluster.calls if method == "PUT"]
    assert puts, "an existing ledger is updated by PUT"
    for body in puts:
        assert body is not None and body["metadata"].get("resourceVersion")


def test_a_ledger_409_rereads_and_retries() -> None:
    worker = f"{PREFIX}/{NS}/worker"
    cluster = FakeBuildCluster(ledger={"repositories": [worker]})
    conflicts = [1]

    def concurrent_append(fake: FakeBuildCluster) -> int | None:
        if conflicts[0]:
            conflicts[0] -= 1
            current = fake.ledger() or {"repositories": []}
            current["repositories"].append(f"{PREFIX}/{NS}/api")
            fake.ledger_text = json.dumps(current)
            fake.resource_version += 1
            return 409
        return None

    cluster.put_hook = concurrent_append
    run_build(cluster)
    assert cluster.ledger() == {"repositories": [worker, f"{PREFIX}/{NS}/api", REPO]}


def test_endless_ledger_conflicts_give_up_before_any_secret() -> None:
    cluster = FakeBuildCluster(ledger={"repositories": []})
    cluster.put_hook = lambda fake: 409
    with pytest.raises(ClusterError):
        run_build(cluster)
    puts = [c for c in cluster.calls if c[0] == "PUT"]
    assert 1 <= len(puts) <= 5
    assert cluster.posted("Secret") == [] and cluster.posted("Job") == []


def test_a_malformed_ledger_is_refused_before_any_secret() -> None:
    cluster = FakeBuildCluster()
    cluster.ledger_text = "{not json"
    with pytest.raises(ClusterError, match="e2e image ledger is malformed"):
        run_build(cluster)
    assert cluster.posted("Secret") == [] and cluster.posted("Job") == []


def test_a_closing_ledger_refuses_with_no_secret_or_job() -> None:
    cluster = FakeBuildCluster(
        ledger={"repositories": [REPO], "closing_at": "2026-10-02T12:00:00Z"}
    )
    with pytest.raises(ClusterError, match="e2e_environment_closing"):
        run_build(cluster)
    assert cluster.posted("Secret") == [] and cluster.posted("Job") == []
    assert [c for c in cluster.calls if c[0] == "PUT"] == []


def test_a_close_interleaving_the_admission_put_is_refused_on_the_reread() -> None:
    cluster = FakeBuildCluster(ledger={"repositories": []})

    def teardown_closes(fake: FakeBuildCluster) -> int | None:
        if "closing_at" not in (fake.ledger() or {}):
            fake.close()
            return 409
        return None

    cluster.put_hook = teardown_closes
    with pytest.raises(ClusterError, match="e2e_environment_closing"):
        run_build(cluster)
    assert cluster.posted("Secret") == [] and cluster.posted("Job") == []
    assert cluster.posted("NetworkPolicy") == []


def test_a_close_seen_after_the_job_post_deletes_the_job_and_secrets() -> None:
    cluster = FakeBuildCluster()
    cluster.after_job_post = FakeBuildCluster.close

    with pytest.raises(ClusterError, match="e2e_environment_closing"):
        run_build(cluster)

    [delete] = cluster.job_deletes()
    method, path, body = delete
    assert urllib.parse.urlsplit(path).path.endswith(f"/jobs/{JOB}")
    assert "propagationPolicy=Background" in path or (
        body is not None and body.get("propagationPolicy") == "Background"
    )
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])
    assert cluster.job_gets == {}


def test_a_ledger_gone_after_the_job_post_deletes_the_job() -> None:
    cluster = FakeBuildCluster()

    def gone(fake: FakeBuildCluster) -> None:
        fake.ledger_text = None

    cluster.after_job_post = gone
    with pytest.raises(ClusterError, match="e2e_environment_closing"):
        run_build(cluster)
    assert len(cluster.job_deletes()) == 1
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])


# Cleanup


def test_a_failed_second_secret_deletes_only_the_first() -> None:
    cluster = FakeBuildCluster()
    cluster.post_status[CACHE_SECRET] = 500
    with pytest.raises(ClusterError):
        run_build(cluster)
    assert cluster.deleted("secrets") == [PUSH_SECRET]
    assert cluster.posted("Job") == []


def test_a_409_on_a_secret_raises_and_cleans_up() -> None:
    cluster = FakeBuildCluster()
    cluster.post_status[CACHE_SECRET] = 409
    with pytest.raises(ClusterError):
        run_build(cluster)
    assert cluster.deleted("secrets") == [PUSH_SECRET]
    assert cluster.posted("Job") == []


def test_a_quota_refused_job_post_deletes_both_secrets() -> None:
    cluster = FakeBuildCluster()
    cluster.post_status[JOB] = 403
    with pytest.raises(ClusterError):
        run_build(cluster)
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])


def test_a_transport_error_while_polling_deletes_both_secrets() -> None:
    class Unreachable(FakeBuildCluster):
        def request(
            self, method: str, path: str, body: dict[str, Any] | None = None
        ) -> tuple[int, dict[str, Any]]:
            if method == "GET" and f"/jobs/{JOB}" in path and self.job_gets.get(JOB, 0) >= 1:
                self.calls.append((method, path, body))
                raise ClusterError("the test cluster API is unreachable")
            return super().request(method, path, body)

    cluster = Unreachable()
    with pytest.raises(ClusterError, match="unreachable"):
        run_build(cluster)
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])


@pytest.mark.parametrize(
    ("push", "cache", "expected"),
    [
        (PUSH_CONFIG, CACHE_CONFIG, [PUSH_SECRET, CACHE_SECRET]),
        (PUSH_CONFIG, None, [PUSH_SECRET]),
        (None, None, []),
    ],
    ids=["both", "push-only", "none"],
)
def test_success_deletes_exactly_the_created_secrets(
    push: str | None, cache: str | None, expected: list[str]
) -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, push=push, cache=cache)
    assert sorted(cluster.deleted("secrets")) == sorted(expected)
    assert cluster.created_secrets() == expected
    assert cluster.job_deletes() == []


def test_a_failed_job_deletes_both_secrets() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [
        ACTIVE,
        failed("BackoffLimitExceeded", "Job has reached the specified backoff limit"),
    ]
    cluster.pods[JOB] = [build_failure_pod("error building image: exit status 1")]
    with pytest.raises(ClusterError, match="e2e_build_failed"):
        run_build(cluster)
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])


def test_the_connector_deadline_deletes_the_job_and_secrets() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [ACTIVE]
    clock = FakeClock()
    polls: list[float] = []

    with pytest.raises(ClusterError, match="e2e_build_timeout"):
        run_build(cluster, cfg=config(timeout_seconds=60), clock=clock, polls=polls)

    # Polling stopped once the clock passed timeout plus 30 s of grace.
    assert clock.now - 1000.0 >= 90
    assert clock.now - 1000.0 <= 90 + 2 * 5
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])
    [delete] = cluster.job_deletes()
    _method, path, body = delete
    assert urllib.parse.urlsplit(path).path.endswith(f"/jobs/{JOB}")
    assert "propagationPolicy=Background" in path or (
        body is not None and body.get("propagationPolicy") == "Background"
    )


# Failure reporting


def build_failure_pod(message: str, *, source_code: int = 0) -> dict[str, Any]:
    init = [terminated("source", source_code)]
    if source_code == 0:
        init.append(terminated("build", 1, message=message))
    else:
        init.append(waiting("build"))
    return {
        "metadata": {"name": f"{JOB}-x7k2p", "labels": {"job-name": JOB}},
        "status": {
            "phase": "Failed",
            "initContainerStatuses": init,
            "containerStatuses": [waiting("push")],
        },
    }


def test_a_build_failure_reports_the_tail_redacted() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [
        failed("BackoffLimitExceeded", "Job has reached the specified backoff limit")
    ]
    noise = "x" * 3000
    message = (
        f"{noise} RUN step leaked {PUSH_PASSWORD} and {CACHE_PASSWORD} and {PUSH_AUTH}; "
        "error building image: RUN-STEP-FAILED-MARKER"
    )
    cluster.pods[JOB] = [build_failure_pod(message)]

    with pytest.raises(ClusterError, match="e2e_build_failed") as excinfo:
        run_build(cluster)

    text = str(excinfo.value)
    assert "RUN-STEP-FAILED-MARKER" in text
    for credential in (PUSH_PASSWORD, CACHE_PASSWORD, PUSH_AUTH):
        assert credential not in text
    assert len(text) <= 1200


def test_a_build_failure_without_a_message_reports_the_reason() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [failed("BackoffLimitExceeded", "backoff limit")]
    pod = build_failure_pod("")
    pod["status"]["initContainerStatuses"][1] = terminated("build", 137, reason="OOMKilled")
    cluster.pods[JOB] = [pod]
    with pytest.raises(ClusterError, match="e2e_build_failed") as excinfo:
        run_build(cluster)
    assert "OOMKilled" in str(excinfo.value)


def test_a_source_failure_is_reported_when_build_never_ran() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [failed("BackoffLimitExceeded", "backoff limit")]
    pod = build_failure_pod("", source_code=128)
    pod["status"]["initContainerStatuses"][0] = terminated(
        "source", 128, message="fatal: remote error: upload-pack: not our ref"
    )
    cluster.pods[JOB] = [pod]
    with pytest.raises(ClusterError, match="e2e_build_failed") as excinfo:
        run_build(cluster)
    assert "not our ref" in str(excinfo.value)


def test_the_job_condition_message_is_the_last_resort() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [failed("PodFailurePolicy", "CONDITION-MESSAGE-MARKER")]
    cluster.pods[JOB] = []
    with pytest.raises(ClusterError, match="e2e_build_failed") as excinfo:
        run_build(cluster)
    assert "CONDITION-MESSAGE-MARKER" in str(excinfo.value)


def test_a_job_deadline_is_a_timeout() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [
        ACTIVE,
        failed("DeadlineExceeded", "Job was active longer than specified deadline"),
    ]
    cluster.pods[JOB] = [build_failure_pod("")]
    with pytest.raises(ClusterError, match="e2e_build_timeout"):
        run_build(cluster)


def test_a_failed_condition_alone_ends_polling() -> None:
    cluster = FakeBuildCluster()
    # No "failed" count yet, only the terminal condition.
    cluster.default_script = [
        {
            "conditions": [
                condition("FailureTarget", reason="BackoffLimitExceeded"),
                condition("Failed", reason="BackoffLimitExceeded", message="m"),
            ]
        }
    ]
    cluster.pods[JOB] = [build_failure_pod("BUILD-MARKER")]
    with pytest.raises(ClusterError, match="e2e_build_failed"):
        run_build(cluster)
    assert cluster.job_gets[JOB] == 1


@pytest.mark.parametrize(
    "message",
    [
        f"{PREFIX}/{NS}/other@{DIGEST}",
        f"{PREFIX}/curie-e2e-elsewhere/app@{DIGEST}",
        f"{REPO}@sha256:xyz",
        f"{REPO}@sha512:{'ab' * 64}",
        f"{REPO}:build-{BUILD_ID}",
        "",
    ],
    ids=["other-repo", "other-namespace", "short-digest", "sha512", "tag-not-digest", "empty"],
)
def test_a_bad_push_message_is_no_digest(message: str) -> None:
    cluster = FakeBuildCluster()
    cluster.push_messages[JOB] = message
    with pytest.raises(ClusterError, match="e2e_build_no_digest"):
        run_build(cluster)
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])


def test_an_environment_removed_mid_poll_is_a_build_failure() -> None:
    cluster = FakeBuildCluster()
    cluster.job_get_error = lambda count: 404 if count >= 1 else None
    with pytest.raises(ClusterError, match="e2e_build_failed"):
        run_build(cluster)
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])


# Options


def test_custom_platform_only_when_given() -> None:
    cluster = FakeBuildCluster()
    run_build(cluster, platforms=["linux/arm64"])
    assert "--custom-platform=linux/arm64" in build_args(cluster)

    cluster = FakeBuildCluster()
    run_build(cluster)
    assert not any(arg.startswith("--custom-platform") for arg in build_args(cluster))


def test_on_poll_is_called_once_per_poll() -> None:
    cluster = FakeBuildCluster()
    cluster.default_script = [ACTIVE, ACTIVE, ACTIVE, SUCCEEDED]
    polls: list[float] = []
    clock = FakeClock()
    run_build(cluster, polls=polls, clock=clock)
    assert len(polls) == cluster.job_gets[JOB] == 4
    assert polls == sorted(polls)
    assert all(seconds == 5 for seconds in clock.sleeps)


# Uncertain Secret creation (review 1, finding 4)

OTHER_BUILD = "e2e-build-ffffffff"


def secret_with_build_label(name: str, build: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "labels": {BUILD_LABEL: build}},
    }


def test_a_secret_persisted_before_a_transport_error_on_its_post_is_still_deleted() -> None:
    cluster = FakeBuildCluster()
    cluster.post_transport_error.add(PUSH_SECRET)
    with pytest.raises(ClusterError, match="unreachable"):
        run_build(cluster)
    assert PUSH_SECRET in cluster.deleted("secrets")
    assert PUSH_SECRET not in cluster.secrets
    assert cluster.posted("Job") == []


def test_a_second_secret_lost_to_a_transport_error_deletes_both_secrets() -> None:
    cluster = FakeBuildCluster()
    cluster.post_transport_error.add(CACHE_SECRET)
    with pytest.raises(ClusterError, match="unreachable"):
        run_build(cluster)
    assert sorted(cluster.deleted("secrets")) == sorted([PUSH_SECRET, CACHE_SECRET])
    assert cluster.secrets == {}


def test_a_secret_with_another_builds_label_is_not_deleted_after_an_uncertain_post() -> None:
    cluster = FakeBuildCluster()
    cluster.secrets[PUSH_SECRET] = secret_with_build_label(PUSH_SECRET, OTHER_BUILD)
    cluster.post_transport_error.add(PUSH_SECRET)
    with pytest.raises(ClusterError, match="unreachable"):
        run_build(cluster)
    assert cluster.deleted("secrets") == []
    assert cluster.secrets[PUSH_SECRET]["metadata"]["labels"][BUILD_LABEL] == OTHER_BUILD


# Unsupported registry credential forms (review 1, finding 2)

TOKEN_ONLY_CONFIG = json.dumps({"auths": {HOST: {"identitytoken": "idtok-SENTINEL"}}})
BAD_AUTH_CONFIG = json.dumps({"auths": {HOST: {"auth": "!!not-base64!!"}}})


@pytest.mark.parametrize("config_text", [TOKEN_ONLY_CONFIG, BAD_AUTH_CONFIG], ids=["token", "auth"])
@pytest.mark.parametrize("which", ["push", "cache"])
def test_an_unsupported_registry_credential_is_refused_before_any_cluster_write(
    config_text: str, which: str
) -> None:
    cluster = FakeBuildCluster()
    push = config_text if which == "push" else PUSH_CONFIG
    cache = config_text if which == "cache" else CACHE_CONFIG
    with pytest.raises(ClusterError, match=r"^e2e_connector_misconfigured") as excinfo:
        run_build(cluster, push=push, cache=cache)
    assert "SENTINEL" not in str(excinfo.value)
    assert cluster.calls == []
