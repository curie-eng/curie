"""deploy, run, logs and events inside the run's namespace (#3247, ADR 0176).

The run's namespace also holds image_build's Jobs, its per build credential
Secrets, the ``e2e-images`` ledger, and env_create's quota, limits, network
policies and connector RoleBinding. deploy refuses anything that would reach
those, and refuses a cluster scoped object outright: that work needs CI, not
this connector. Every deploy refusal happens before the first write, so a
refused manifest applies nothing.

Every name and group version that goes into a URL path is checked against the
Kubernetes name grammar first, so manifest text can never add a path segment
or a query.
"""

from __future__ import annotations

import copy
import json
import re
import urllib.parse
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import yaml

from curie_e2e_connector.contract import (
    BUILD_LABEL,
    BUILD_SECRET_PREFIX,
    IMAGES_CONFIGMAP,
    OUTPUT_LIMIT_BYTES,
    REFUSAL_CLUSTER_SCOPED,
    REFUSAL_DEPLOY_FAILED,
    REFUSAL_DEPLOY_MANIFEST,
    REFUSAL_DEPLOY_OBJECT,
    REFUSAL_IMAGE_NOT_DIGEST,
    REFUSAL_LOGS_REFUSED,
    REFUSAL_MISCONFIGURED,
    REFUSAL_NOT_OWNED,
    REFUSAL_POD_NOT_FOUND,
    REFUSAL_RUN_ARGUMENT,
    REFUSAL_RUN_FAILED,
    REFUSAL_RUN_TIMEOUT,
    RUN_JOB_PREFIX,
    RUN_TIMEOUT_S,
)
from curie_e2e_connector.kube import (
    ClusterApi,
    ClusterError,
    checked_request,
    checked_text,
    container_statuses,
    delete_quietly,
    job_pods,
    job_state,
    tail_text,
    terminated_state,
    waiting_reason,
)
from curie_e2e_connector.namespace import (
    Caller,
    Install,
    namespace_name,
    require_caller,
    require_environment,
    run_labels,
)

MAX_MANIFEST_BYTES = 1024 * 1024
MAX_OBJECTS = 100
MAX_COMMAND_ARGS = 64
MAX_ARG_CHARS = 4096
DEFAULT_LOG_TAIL = 500
MAX_LOG_TAIL = 5000
MAX_EVENTS = 200
_JOB_TTL_S = 600
# Polling stops this long after the Job's own activeDeadlineSeconds.
_DEADLINE_GRACE_S = 30
_DETAIL_LIMIT = 500
_EVENT_MESSAGE_LIMIT = 1000

_DNS_SUBDOMAIN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
_KIND = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,62}$")
_RUN_ID = re.compile(r"^[a-z0-9]{1,16}$")
# ``<ref>@sha256:<64 hex>``; a tag before the digest is allowed, because the
# digest pins it.
_DIGEST_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")

_CONTAINER_LISTS = ("containers", "initContainers", "ephemeralContainers")
_SERVER_METADATA = (
    "resourceVersion",
    "uid",
    "creationTimestamp",
    "managedFields",
    "generation",
    "selfLink",
)
_RBAC = "rbac.authorization.k8s.io"
# Waiting reasons that will not clear on their own. ErrImagePull is absent on
# purpose: the kubelet retries it, and it often clears.
_FATAL_WAITING = frozenset({"InvalidImageName", "ErrImageNeverPull", "ImagePullBackOff"})

# (group, kind) the apiserver serves cluster scoped. Every other kind is
# resolved by discovery.
CLUSTER_SCOPED: frozenset[tuple[str, str]] = frozenset(
    {
        ("", "Namespace"),
        ("", "Node"),
        ("", "PersistentVolume"),
        ("", "ComponentStatus"),
        ("apiextensions.k8s.io", "CustomResourceDefinition"),
        ("scheduling.k8s.io", "PriorityClass"),
        (_RBAC, "ClusterRole"),
        (_RBAC, "ClusterRoleBinding"),
        ("admissionregistration.k8s.io", "ValidatingWebhookConfiguration"),
        ("admissionregistration.k8s.io", "MutatingWebhookConfiguration"),
        ("admissionregistration.k8s.io", "ValidatingAdmissionPolicy"),
        ("admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding"),
        ("admissionregistration.k8s.io", "MutatingAdmissionPolicy"),
        ("admissionregistration.k8s.io", "MutatingAdmissionPolicyBinding"),
        ("storage.k8s.io", "StorageClass"),
        ("storage.k8s.io", "CSIDriver"),
        ("storage.k8s.io", "CSINode"),
        ("storage.k8s.io", "VolumeAttachment"),
        ("apiregistration.k8s.io", "APIService"),
        ("networking.k8s.io", "IngressClass"),
        ("node.k8s.io", "RuntimeClass"),
        ("certificates.k8s.io", "CertificateSigningRequest"),
        ("flowcontrol.apiserver.k8s.io", "FlowSchema"),
        ("flowcontrol.apiserver.k8s.io", "PriorityLevelConfiguration"),
    }
)


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)


def _valid_name(value: Any) -> bool:
    return isinstance(value, str) and len(value) <= 253 and bool(_DNS_SUBDOMAIN.fullmatch(value))


def _valid_label(value: Any) -> bool:
    return isinstance(value, str) and bool(_DNS_LABEL.fullmatch(value))


def is_digest_image(image: Any) -> bool:
    return isinstance(image, str) and bool(_DIGEST_IMAGE.fullmatch(image))


def _nodes(value: Any) -> Iterator[Any]:
    """Every dict, list and scalar under ``value``, without recursion."""

    stack = [value]
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, dict):
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


# Manifest parsing


class _NoAliasLoader(yaml.SafeLoader):
    """SafeLoader that refuses anchors and aliases.

    An alias is a shared reference, so a small document can name a structure
    whose walk is exponential. Manifests never need them.
    """

    def compose_node(self, parent: Any, index: Any) -> Any:
        if self.check_event(yaml.AliasEvent):
            raise yaml.composer.ComposerError(problem="aliases are not accepted")
        return super().compose_node(parent, index)


def _manifest_refused(reason: str) -> ClusterError:
    return ClusterError(f"{REFUSAL_DEPLOY_MANIFEST}: {reason}")


@dataclass(frozen=True)
class _Object:
    body: dict[str, Any]
    api_version: str
    group: str
    version: str
    kind: str
    name: str

    def entry(self) -> dict[str, str]:
        return {"kind": self.kind, "name": self.name}


def parse_manifests(text: str) -> list[_Object]:
    """Objects in manifest order, ``kind: *List`` documents flattened."""

    if not isinstance(text, str) or not text.strip():
        raise _manifest_refused("manifests is empty")
    if len(text.encode("utf-8")) > MAX_MANIFEST_BYTES:
        raise _manifest_refused(f"manifests exceeds {MAX_MANIFEST_BYTES} bytes")
    try:
        documents = list(yaml.load_all(text, Loader=_NoAliasLoader))  # noqa: S506
    except yaml.YAMLError as exc:
        reason = getattr(exc, "problem", None) or "the YAML does not parse"
        raise _manifest_refused(f"YAML: {reason}") from None
    except RecursionError:
        raise _manifest_refused("the YAML nests too deeply") from None

    raw: list[Any] = []
    for index, document in enumerate(documents):
        if document is None:
            continue
        if not isinstance(document, dict):
            raise _manifest_refused(f"document {index} is not a mapping")
        kind = document.get("kind")
        if isinstance(kind, str) and kind.endswith("List") and "items" in document:
            items = document["items"]
            if not isinstance(items, list):
                raise _manifest_refused(f"document {index} items is not a list")
            raw.extend(items)
        else:
            raw.append(document)
    if not raw:
        raise _manifest_refused("manifests holds no objects")
    if len(raw) > MAX_OBJECTS:
        raise _manifest_refused(f"manifests holds more than {MAX_OBJECTS} objects")
    return [_object(index, item) for index, item in enumerate(raw)]


def _object(index: int, item: Any) -> _Object:
    if not isinstance(item, dict):
        raise _manifest_refused(f"object {index} is not a mapping")
    api_version = item.get("apiVersion")
    kind = item.get("kind")
    metadata = item.get("metadata")
    if not isinstance(api_version, str) or not api_version:
        raise _manifest_refused(f"object {index} has no apiVersion")
    if not isinstance(kind, str) or not _KIND.fullmatch(kind):
        raise _manifest_refused(f"object {index} has no valid kind")
    if not isinstance(metadata, dict):
        raise _manifest_refused(f"object {index} has no metadata")
    name = metadata.get("name")
    if not _valid_name(name):
        raise _manifest_refused(
            f"object {index} metadata.name must be a DNS subdomain of at most 253 characters"
        )
    assert isinstance(name, str)
    group, sep, version = api_version.rpartition("/")
    if not sep:
        group, version = "", api_version
    if (group and not _valid_name(group)) or not _valid_label(version):
        raise _manifest_refused(f"object {index} apiVersion is not <group>/<version>")
    labels = metadata.get("labels")
    if labels is not None and not isinstance(labels, dict):
        raise _manifest_refused(f"object {index} metadata.labels is not a mapping")
    namespace = metadata.get("namespace")
    if namespace is not None and not isinstance(namespace, str):
        raise _manifest_refused(f"object {index} metadata.namespace is not a string")
    return _Object(item, api_version, group, version, kind, name)


# Discovery


@dataclass(frozen=True)
class _Resource:
    plural: str
    namespaced: bool


class _Discovery:
    """APIResourceList per group version, read at most once per deploy."""

    def __init__(self, cluster: ClusterApi) -> None:
        self._cluster = cluster
        self._cache: dict[str, dict[str, _Resource]] = {}

    def resolve(self, obj: _Object) -> _Resource | None:
        return self._resources(obj.group, obj.version).get(obj.kind)

    def _resources(self, group: str, version: str) -> dict[str, _Resource]:
        key = f"{group}/{version}"
        if key in self._cache:
            return self._cache[key]
        path = f"/apis/{group}/{version}" if group else f"/api/{version}"
        code, payload = checked_request(self._cluster, "GET", path)
        found: dict[str, _Resource] = {}
        if code == 200:
            items = payload.get("resources")
            for item in items if isinstance(items, list) else []:
                if not isinstance(item, dict):
                    continue
                plural, kind = item.get("name"), item.get("kind")
                # ``pods/log`` and ``deployments/status`` are subresources.
                if not _valid_label(plural) or not isinstance(kind, str):
                    continue
                assert isinstance(plural, str)
                found.setdefault(kind, _Resource(plural, item.get("namespaced") is True))
        elif code != 404:
            raise ClusterError(f"the test cluster did not answer discovery for {key} ({code})")
        self._cache[key] = found
        return found


def _collection(namespace: str, obj: _Object, resource: _Resource) -> str:
    base = f"/apis/{obj.group}/{obj.version}" if obj.group else f"/api/{obj.version}"
    return f"{base}/namespaces/{namespace}/{resource.plural}"


# Content checks


def _image_problems(obj: _Object) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []

    def check(container: str, image: Any) -> None:
        if not is_digest_image(image):
            found.append(
                {
                    "kind": obj.kind,
                    "name": obj.name,
                    "container": container,
                    "image": image if isinstance(image, str) else "",
                }
            )

    for node in _nodes(obj.body):
        if not isinstance(node, dict):
            continue
        for key in _CONTAINER_LISTS:
            entries = node.get(key)
            for entry in entries if isinstance(entries, list) else []:
                if isinstance(entry, dict):
                    name = entry.get("name")
                    check(name if isinstance(name, str) else "", entry.get("image"))
        volumes = node.get("volumes")
        for volume in volumes if isinstance(volumes, list) else []:
            source = volume.get("image") if isinstance(volume, dict) else None
            if isinstance(source, dict):
                name = volume.get("name")
                check(f"volume/{name if isinstance(name, str) else ''}", source.get("reference"))
    return found


def _rule_reaches(rule: Any, groups: tuple[str, ...], resources: tuple[str, ...]) -> bool:
    if not isinstance(rule, dict):
        return True
    rule_groups = rule.get("apiGroups") or []
    rule_resources = rule.get("resources") or []
    if not isinstance(rule_groups, list) or not isinstance(rule_resources, list):
        return True
    if not any(group == "*" or group in groups for group in rule_groups):
        return False
    for resource in rule_resources:
        if not isinstance(resource, str):
            return True
        base = resource.split("/", 1)[0]
        if base == "*" or base in resources:
            return True
    return False


_READ_VERBS = frozenset({"get", "list", "watch"})


def _dangerous_role(body: dict[str, Any]) -> str:
    """Why a Role is refused, or "" when it is not.

    A deployed pod running under the Role could reach build pods, build
    credentials, or env_create's bounds, so a Role may grant only get, list
    and watch, and never on secrets.
    """

    rules = body.get("rules") or []
    if not isinstance(rules, list):
        return "rules are not a list"
    for rule in rules:
        if not isinstance(rule, dict):
            return "a rule is not a mapping"
        verbs = rule.get("verbs") or []
        if not isinstance(verbs, list) or not all(isinstance(verb, str) for verb in verbs):
            return "a rule's verbs are not a list of strings"
        extra = sorted(set(verbs) - _READ_VERBS)
        if extra:
            return (
                f"the Role grants {', '.join(extra)}; a Role may grant only get, list and "
                "watch, because a pod running under it could reach build pods, credentials, "
                "or env_create's bounds"
            )
        if _rule_reaches(rule, ("",), ("secrets",)):
            return (
                "the Role reaches secrets, which hold the build credentials a pod running "
                "under it could read"
            )
    return ""


def _role_ref(obj: _Object) -> tuple[str, str]:
    ref = obj.body.get("roleRef")
    if not isinstance(ref, dict):
        return "", ""
    kind, name = ref.get("kind"), ref.get("name")
    return (kind if isinstance(kind, str) else "", name if isinstance(name, str) else "")


def _static_reasons(obj: _Object, roles: dict[str, _Object]) -> list[str]:
    reasons: list[str] = []
    if any(
        (isinstance(node, dict) and BUILD_LABEL in node) or node == BUILD_LABEL
        for node in _nodes(obj.body)
    ):
        reasons.append(f"it carries {BUILD_LABEL}, which marks image_build objects")
    if any(
        isinstance(node, str) and node.startswith(BUILD_SECRET_PREFIX) for node in _nodes(obj.body)
    ):
        reasons.append(f"it names {BUILD_SECRET_PREFIX}*, the build credential Secrets")
    if obj.group == "" and obj.kind == "ConfigMap" and obj.name == IMAGES_CONFIGMAP:
        reasons.append("the image ledger belongs to image_build and teardown")
    if obj.group == "networking.k8s.io" and obj.kind == "NetworkPolicy":
        reasons.append("network policy belongs to env_create; use its allow rules")
    if obj.group == "" and obj.kind in ("ResourceQuota", "LimitRange"):
        reasons.append("the namespace bounds belong to env_create")
    if obj.group == _RBAC and obj.kind == "Role":
        why = _dangerous_role(obj.body)
        if why:
            reasons.append(why)
    if obj.group == _RBAC and obj.kind == "RoleBinding":
        if obj.name == "e2e-connector":
            reasons.append("the e2e-connector RoleBinding belongs to env_create")
        ref_kind, ref_name = _role_ref(obj)
        if ref_kind != "Role":
            reasons.append("a RoleBinding may reference only a Role in this namespace")
        elif not _valid_name(ref_name):
            reasons.append("roleRef name is not a DNS subdomain")
        elif ref_name in roles and _dangerous_role(roles[ref_name].body):
            reasons.append(f"it binds Role {ref_name}, which is refused")
    return reasons


def _refuse_objects(found: list[dict[str, str]]) -> ClusterError:
    return ClusterError(f"{REFUSAL_DEPLOY_OBJECT}: {_json({'objects': found})}")


def _write_body(
    obj: _Object, namespace: str, labels: dict[str, str], resource_version: str | None
) -> dict[str, Any]:
    body = copy.deepcopy(obj.body)
    body.pop("status", None)
    metadata = body["metadata"]
    for key in _SERVER_METADATA:
        metadata.pop(key, None)
    metadata["namespace"] = namespace
    metadata["labels"] = {**(metadata.get("labels") or {}), **labels}
    if resource_version is not None:
        metadata["resourceVersion"] = resource_version
    return body


def _status_message(payload: dict[str, Any]) -> str:
    message = payload.get("message")
    return message[-_DETAIL_LIMIT:] if isinstance(message, str) else ""


# deploy


def deploy(cluster: ClusterApi, install: Install, caller: Caller, manifests: str) -> dict[str, Any]:
    """Apply ``manifests`` in the run's namespace.

    Returns ``{"namespace", "applied": [{"kind", "name"}]}`` in manifest order.
    """

    require_caller(caller.run, caller.work_item)
    install.validate()
    namespace = namespace_name(install, caller)
    require_environment(cluster, install, caller, "deploy")
    objects = parse_manifests(manifests)

    # Scope first: a cluster scoped object answers its own code whatever else
    # is wrong, because the factory falls back to CI only proof on it.
    discovery = _Discovery(cluster)
    scoped: list[dict[str, str]] = []
    unknown: list[dict[str, str]] = []
    resources: list[_Resource] = []
    for obj in objects:
        if (obj.group, obj.kind) in CLUSTER_SCOPED:
            scoped.append({"apiVersion": obj.api_version, "kind": obj.kind, "name": obj.name})
            continue
        resource = discovery.resolve(obj)
        if resource is None:
            unknown.append(
                {
                    **obj.entry(),
                    "reason": f"the cluster does not serve {obj.kind} in {obj.api_version}",
                }
            )
            continue
        if not resource.namespaced:
            scoped.append({"apiVersion": obj.api_version, "kind": obj.kind, "name": obj.name})
            continue
        resources.append(resource)
    if scoped:
        raise ClusterError(f"{REFUSAL_CLUSTER_SCOPED}: {_json({'objects': scoped})}")
    if unknown:
        raise _refuse_objects(unknown)

    for obj in objects:
        target = obj.body["metadata"].get("namespace")
        if target is not None and target != namespace:
            raise ClusterError(
                f"{REFUSAL_NOT_OWNED}: {obj.kind}/{obj.name} names namespace {target}; "
                "deploy acts only in the namespace this run created"
            )

    images = [problem for obj in objects for problem in _image_problems(obj)]
    if images:
        raise ClusterError(f"{REFUSAL_IMAGE_NOT_DIGEST}: {_json({'images': images})}")

    roles = {obj.name: obj for obj in objects if obj.group == _RBAC and obj.kind == "Role"}
    refused = [
        {**obj.entry(), "reason": "; ".join(why)}
        for obj in objects
        if (why := _static_reasons(obj, roles))
    ]
    if refused:
        raise _refuse_objects(refused)

    # Read what exists. A build object of the same name, or a binding to an
    # existing Role that is refused, refuses the whole deploy.
    existing: dict[str, dict[str, Any]] = {}
    paths = [
        f"{_collection(namespace, obj, res)}/{obj.name}"
        for obj, res in zip(objects, resources, strict=True)
    ]
    for path in paths:
        code, payload = checked_request(cluster, "GET", path)
        if code == 200:
            existing[path] = payload
        elif code != 404:
            raise ClusterError(f"the test cluster did not answer a read of {path} ({code})")
    roles_path = f"/apis/{_RBAC}/v1/namespaces/{namespace}/roles"
    for obj, path in zip(objects, paths, strict=True):
        reasons: list[str] = []
        metadata = existing.get(path, {}).get("metadata")
        labels = metadata.get("labels") if isinstance(metadata, dict) else None
        if isinstance(labels, dict) and BUILD_LABEL in labels:
            reasons.append(f"the existing {obj.kind} belongs to image_build")
        if obj.group == _RBAC and obj.kind == "RoleBinding":
            ref_name = _role_ref(obj)[1]
            role_path = f"{roles_path}/{ref_name}"
            if role_path not in existing:
                code, payload = checked_request(cluster, "GET", role_path)
                if code == 200:
                    existing[role_path] = payload
                elif code != 404:
                    raise ClusterError(
                        f"the test cluster did not answer a read of Role {ref_name} ({code})"
                    )
            if role_path in existing and _dangerous_role(existing[role_path]):
                reasons.append(f"it binds the existing Role {ref_name}, which is refused")
        if reasons:
            refused.append({**obj.entry(), "reason": "; ".join(reasons)})
    if refused:
        raise _refuse_objects(refused)

    labels = run_labels(install, caller)
    applied: list[dict[str, str]] = []
    for obj, resource, path in zip(objects, resources, paths, strict=True):
        current = existing.get(path)
        version: str | None = None
        if current is not None:
            metadata = current.get("metadata")
            stored = metadata.get("resourceVersion") if isinstance(metadata, dict) else None
            version = stored if isinstance(stored, str) else None
        body = _write_body(obj, namespace, labels, version)
        if current is None:
            method, target, ok = "POST", _collection(namespace, obj, resource), (200, 201)
        else:
            method, target, ok = "PUT", path, (200, 201)
        try:
            code, payload = cluster.request(method, target, body)
        except ClusterError:
            raise ClusterError(
                f"{REFUSAL_DEPLOY_FAILED}: {obj.kind}/{obj.name} (unreachable) "
                f"the test cluster API did not answer; applied: {json.dumps(applied)}"
            ) from None
        if code not in ok:
            raise ClusterError(
                f"{REFUSAL_DEPLOY_FAILED}: {obj.kind}/{obj.name} ({code}) "
                f"{_status_message(payload)}; applied: {json.dumps(applied)}"
            )
        applied.append(obj.entry())
    return {"namespace": namespace, "applied": applied}


# run


def _run_refused(reason: str) -> ClusterError:
    return ClusterError(f"{REFUSAL_RUN_ARGUMENT}: {reason}")


def _check_command(command: Any) -> list[str]:
    if not isinstance(command, list) or not 1 <= len(command) <= MAX_COMMAND_ARGS:
        raise _run_refused(f"command must be a list of 1 to {MAX_COMMAND_ARGS} strings")
    for arg in command:
        if not isinstance(arg, str) or len(arg) > MAX_ARG_CHARS:
            raise _run_refused(f"each command entry must be a string of at most {MAX_ARG_CHARS}")
    return list(command)


def _run_job_body(
    job: str, image: str, command: list[str], install: Install, labels: dict[str, str]
) -> dict[str, Any]:
    security: dict[str, Any] = {
        "allowPrivilegeEscalation": False,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    if install.pod_security == "restricted":
        security["runAsNonRoot"] = True
        security["capabilities"] = {"drop": ["ALL"]}
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job, "labels": labels},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": RUN_TIMEOUT_S,
            "ttlSecondsAfterFinished": _JOB_TTL_S,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "containers": [
                        {
                            "name": "run",
                            "image": image,
                            "command": command,
                            "securityContext": security,
                        }
                    ],
                },
            },
        },
    }


def _fatal_waiting(pods: list[dict[str, Any]]) -> str:
    for pod in pods:
        for key in ("initContainerStatuses", "containerStatuses"):
            for container in container_statuses(pod, key):
                reason = waiting_reason(container)
                if reason in _FATAL_WAITING:
                    return reason
    return ""


def _run_result(pods: list[dict[str, Any]]) -> tuple[str, dict[str, Any]] | None:
    """The pod name and terminated state of the ``run`` container, if any."""

    for pod in pods:
        metadata = pod.get("metadata")
        name = metadata.get("name") if isinstance(metadata, dict) else None
        if not _valid_name(name):
            continue
        assert isinstance(name, str)
        for container in container_statuses(pod, "containerStatuses"):
            if container.get("name") != "run":
                continue
            terminated = terminated_state(container)
            if terminated is not None:
                return name, terminated
    return None


def run_command(
    cluster: ClusterApi,
    install: Install,
    caller: Caller,
    command: list[str],
    image: str,
    *,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    run_id: str,
    poll_seconds: float = 5,
    on_poll: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Run ``command`` once in ``image`` as a Job and return its exit and output.

    Returns ``{"exit_code", "stdout", "stderr"}``. Kubernetes merges a
    container's stdout and stderr into one log, so ``stdout`` is that log and
    ``stderr`` is the termination message or reason. A non zero exit is a
    result, not an error. The Job is deleted on every path.
    """

    require_caller(caller.run, caller.work_item)
    install.validate()
    if not is_digest_image(image):
        raise ClusterError(
            f"{REFUSAL_IMAGE_NOT_DIGEST}: image must be <ref>@sha256:<64 hex>; "
            "image_build returns the digest"
        )
    args = _check_command(command)
    if not _RUN_ID.fullmatch(run_id):
        raise ClusterError(f"{REFUSAL_MISCONFIGURED}: run id is not a short lowercase token")
    if poll_seconds <= 0:
        raise ClusterError(f"{REFUSAL_MISCONFIGURED}: run poll interval must be positive")
    namespace = namespace_name(install, caller)
    require_environment(cluster, install, caller, "run")

    job = f"{RUN_JOB_PREFIX}{run_id}"
    jobs = f"/apis/batch/v1/namespaces/{namespace}/jobs"
    body = _run_job_body(job, image, args, install, run_labels(install, caller))
    # Set before the POST: a lost response may still have created the Job.
    delete_job = True
    try:
        code, _payload = checked_request(cluster, "POST", jobs, body)
        if code == 409:
            delete_job = False
            raise ClusterError(f"{REFUSAL_RUN_FAILED}: a Job named {job} already exists")
        if code not in (200, 201):
            raise ClusterError(f"the test cluster refused the run Job ({code})")
        return _await_run(
            cluster,
            namespace,
            job,
            clock=clock,
            sleep=sleep,
            poll_seconds=poll_seconds,
            on_poll=on_poll,
        )
    finally:
        if delete_job:
            delete_quietly(cluster, f"{jobs}/{job}?propagationPolicy=Background", "a run Job")


def _await_run(
    cluster: ClusterApi,
    namespace: str,
    job: str,
    *,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    poll_seconds: float,
    on_poll: Callable[[float], None] | None,
) -> dict[str, Any]:
    start = clock()
    limit = start + RUN_TIMEOUT_S + _DEADLINE_GRACE_S
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs/{job}"
    while True:
        code, payload = checked_request(cluster, "GET", path)
        if on_poll is not None:
            on_poll(clock() - start)
        if code == 404:
            raise ClusterError(f"{REFUSAL_RUN_FAILED}: the run Job was removed")
        if code != 200:
            raise ClusterError(f"the test cluster did not answer a run Job read ({code})")
        state = job_state(payload.get("status"))
        if state is not None:
            break
        fatal = _fatal_waiting(job_pods(cluster, namespace, job))
        if fatal:
            raise ClusterError(f"{REFUSAL_RUN_FAILED}: the run container is stuck in {fatal}")
        if clock() >= limit:
            raise _run_timeout()
        sleep(poll_seconds)

    succeeded, reason, message = state
    # activeDeadlineSeconds kills the running container, so a deadline is a
    # timeout even when that container reports an exit.
    if not succeeded and reason == "DeadlineExceeded":
        raise _run_timeout()
    failed = ClusterError(
        f"{REFUSAL_RUN_FAILED}: {(message or reason or 'the run Job failed')[-_DETAIL_LIMIT:]}"
    )
    try:
        found = _run_result(_finished_run_pods(cluster, namespace, job))
    except ClusterError:
        if not succeeded:
            raise failed from None
        raise
    if found is None:
        if not succeeded:
            raise failed
        raise ClusterError(f"{REFUSAL_RUN_FAILED}: the run container reported no exit")
    pod, terminated = found
    exit_code = terminated.get("exitCode")
    exit_code = exit_code if isinstance(exit_code, int) else (0 if succeeded else 1)
    term_message = terminated.get("message")
    term_reason = terminated.get("reason")
    if isinstance(term_message, str) and term_message:
        stderr = term_message
    elif exit_code != 0 and isinstance(term_reason, str):
        stderr = term_reason
    else:
        stderr = ""
    log_code, text = checked_text(
        cluster, f"/api/v1/namespaces/{namespace}/pods/{pod}/log?container=run"
    )
    if log_code != 200:
        raise ClusterError(
            f"{REFUSAL_RUN_FAILED}: the test cluster did not return the run log ({log_code})"
        )
    stdout = text
    return {
        "exit_code": exit_code,
        "stdout": tail_text(stdout.encode("utf-8"), OUTPUT_LIMIT_BYTES),
        "stderr": tail_text(stderr.encode("utf-8"), OUTPUT_LIMIT_BYTES),
    }


def _finished_run_pods(cluster: ClusterApi, namespace: str, job: str) -> list[dict[str, Any]]:
    """The finished run Job's pods; unlike ``job_pods``, a failed read or none is an error."""

    selector = urllib.parse.quote(f"job-name={job}", safe="")
    code, payload = checked_request(
        cluster, "GET", f"/api/v1/namespaces/{namespace}/pods?labelSelector={selector}"
    )
    if code != 200:
        raise ClusterError(
            f"{REFUSAL_RUN_FAILED}: the test cluster did not list the run pods ({code})"
        )
    items = payload.get("items")
    pods = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    if not pods:
        raise ClusterError(f"{REFUSAL_RUN_FAILED}: the finished run Job has no pods")
    return pods


def _run_timeout() -> ClusterError:
    return ClusterError(f"{REFUSAL_RUN_TIMEOUT}: the run did not finish within {RUN_TIMEOUT_S}s")


# logs


def read_logs(
    cluster: ClusterApi,
    install: Install,
    caller: Caller,
    pod: str,
    container: str | None,
    tail: int | None,
) -> dict[str, Any]:
    """The last lines of one pod's log, ``{"logs": "..."}``.

    A build pod is refused: its output would bypass image_build's credential
    redaction.
    """

    require_caller(caller.run, caller.work_item)
    install.validate()
    if not _valid_name(pod):
        raise ClusterError(f"{REFUSAL_POD_NOT_FOUND}: pod must be a DNS subdomain name")
    if container is not None and not _valid_label(container):
        raise ClusterError("logs container must be a DNS label")
    lines = DEFAULT_LOG_TAIL if tail is None else tail
    if isinstance(lines, bool) or not isinstance(lines, int) or not 1 <= lines <= MAX_LOG_TAIL:
        raise ClusterError(f"logs tail must be between 1 and {MAX_LOG_TAIL}")
    namespace = namespace_name(install, caller)
    require_environment(cluster, install, caller, "logs")

    pod_path = f"/api/v1/namespaces/{namespace}/pods/{pod}"
    code, payload = checked_request(cluster, "GET", pod_path)
    if code == 404:
        raise ClusterError(f"{REFUSAL_POD_NOT_FOUND}: no pod {pod} in this run's namespace")
    if code != 200:
        raise ClusterError(f"the test cluster did not answer a pod read ({code})")
    metadata = payload.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    if isinstance(labels, dict) and BUILD_LABEL in labels:
        raise ClusterError(
            f"{REFUSAL_LOGS_REFUSED}: {pod} is an image_build pod; image_build reports its failures"
        )

    query: dict[str, str] = {"tailLines": str(lines)}
    if container is not None:
        query["container"] = container
    log_code, text = checked_text(cluster, f"{pod_path}/log?{urllib.parse.urlencode(query)}")
    if log_code != 200:
        raise ClusterError(
            f"the test cluster did not return the log ({log_code}) {text[-_DETAIL_LIMIT:]}"
        )
    return {"logs": tail_text(text.encode("utf-8"), OUTPUT_LIMIT_BYTES)}


# events


def _event_stamp(item: dict[str, Any]) -> str:
    for key in ("lastTimestamp", "eventTime"):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    metadata = item.get("metadata")
    created = metadata.get("creationTimestamp") if isinstance(metadata, dict) else None
    return created if isinstance(created, str) else ""


def _event_count(item: dict[str, Any]) -> int:
    count = item.get("count")
    if isinstance(count, int) and not isinstance(count, bool) and count > 0:
        return count
    series = item.get("series")
    count = series.get("count") if isinstance(series, dict) else None
    if isinstance(count, int) and not isinstance(count, bool) and count > 0:
        return count
    return 1


def list_events(
    cluster: ClusterApi, install: Install, caller: Caller, involved: str | None
) -> dict[str, Any]:
    """The run namespace's latest events, oldest first, optionally for one object."""

    require_caller(caller.run, caller.work_item)
    install.validate()
    if involved is not None and not _valid_name(involved):
        raise ClusterError("events involved must be a DNS subdomain name")
    namespace = namespace_name(install, caller)
    require_environment(cluster, install, caller, "events")

    path = f"/api/v1/namespaces/{namespace}/events"
    if involved is not None:
        path += "?" + urllib.parse.urlencode({"fieldSelector": f"involvedObject.name={involved}"})
    code, payload = checked_request(cluster, "GET", path)
    if code != 200:
        raise ClusterError(f"the test cluster did not answer an event list ({code})")
    items = payload.get("items")
    events = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    events.sort(key=_event_stamp)

    def text(item: dict[str, Any], key: str) -> str:
        value = item.get(key)
        return value[:_EVENT_MESSAGE_LIMIT] if isinstance(value, str) else ""

    return {
        "events": [
            {
                "reason": text(item, "reason"),
                "message": text(item, "message"),
                "type": text(item, "type"),
                "count": _event_count(item),
            }
            for item in events[-MAX_EVENTS:]
        ]
    }
