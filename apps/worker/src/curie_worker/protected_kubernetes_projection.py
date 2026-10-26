"""Closed kubernetes-cold-pod-v1 projection, not runtime qualification.

The projection is structural evidence. Pure rendering/extraction and immutable
profile matching do not authenticate observations, issue Secret assignments,
establish preventive controls or authorize an operation.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from urllib.parse import quote

from aci_protocol import BootEnv, Budget
from curie_protected_hooks.artifact_measurement import (
    _DELIVERY_SLOTS,
    ArtifactMeasurement,
    _fingerprint,
    _open_root,
)
from curie_protected_hooks.authority_records import _CREDENTIAL, Manifest, _object, _scalar
from curie_protected_hooks.worker_verification import Dispatchable
from plugin_format import is_reserved_boot_env_name

from .protected_configuration import ProtectedExecutionConfig, parse_execution_config
from .protected_history import protected_history_key
from .protected_launch_records import (
    _decode,
    _pattern,
    _require,
    _text,
    _timestamp,
    canonical_json,
    safe_validation,
)

_MAX_BYTES = 262144
_OBSERVATION_CAP = 1048576
_FORMAT = "kubernetes-cold-pod-v1"
_LAUNCH_LABEL = "curietech.ai/protected-launch"
_TOP = {
    "schema_version",
    "format",
    "authority_domain_id",
    "launch_identity",
    "namespace",
    "service_account",
    "runtime_class",
    "image",
    "embedded_bundle",
    "platform",
    "image_environment",
    "environment",
    "model_credential_ref",
    "history_signing_credential_ref",
    "resources",
    "network_policy_ref",
    "delivery_slots",
}
_STATIC_FIELDS = (
    "plugin_dir",
    "budget",
    "bundle_ref",
    "model",
    "reviewer_model",
    "fake_model",
    "base_url",
    "api_backend",
    "thinking",
    "model_env_key",
    "max_turns",
    "memory_writes",
)
_INTERNAL_STATIC = {"CURIE_HARNESS": "claude-agent-sdk", "CURIE_FALSE_COMPLETION_CHECK": "0"}
_IMAGE_FIXED = {
    "HOME": "/home/runner",
    BootEnv.env_key("port"): "8080",
    "CURIE_PROC_DUMPABLE_PRELOAD": "/usr/local/lib/curie/libproc_dumpable.so",
}
_METADATA = {
    "name",
    "namespace",
    "uid",
    "resourceVersion",
    "creationTimestamp",
    "managedFields",
    "labels",
    "finalizers",
    "ownerReferences",
    "generation",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
}
_MANAGED_FIELDS = {
    "manager",
    "operation",
    "apiVersion",
    "time",
    "fieldsType",
    "fieldsV1",
    "subresource",
}
_EMPTY_SPEC = {
    "initContainers",
    "ephemeralContainers",
    "imagePullSecrets",
    "schedulingGates",
    "topologySpreadConstraints",
}
_EMPTY_CONTAINER = {"envFrom", "volumeDevices"}


class ProjectionInvalid(ValueError):
    """Fixed refusal with all source exception links detached."""

    def __init__(self) -> None:
        super().__init__("Invalid protected Kubernetes projection")


def _closed(value: Any, keys: set[str]) -> dict[str, Any]:
    _require(type(value) is dict and value.keys() == keys)
    return value  # type: ignore[no-any-return]


def _integer(value: Any, low: int, high: int) -> None:
    _require(type(value) is int and low <= value <= high)


def _environment(value: Any) -> dict[str, str]:
    _require(type(value) is list and len(value) <= 64)
    result: dict[str, str] = {}
    for item in value:
        entry = _closed(item, {"name", "value"})
        _pattern(entry["name"], r"[A-Z_][A-Z0-9_]{0,127}")
        _text(entry["value"], 8192, empty=True)
        _require(entry["name"] not in result)
        result[entry["name"]] = entry["value"]
    _require(list(result) == sorted(result))
    return result


def _entries(value: dict[str, str]) -> list[dict[str, str]]:
    return [{"name": name, "value": content} for name, content in sorted(value.items())]


def _validate(value: Any) -> None:
    v = _closed(value, _TOP)
    _scalar(v["schema_version"], "version")
    _require(type(v["format"]) is str and v["format"] == _FORMAT)
    _scalar(v["authority_domain_id"], "uuid")
    _scalar(v["launch_identity"], "opaque_ref")
    for name in ("namespace", "service_account", "runtime_class"):
        _pattern(v[name], r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
    image = _closed(v["image"], {"reference", "digest"})
    _scalar(image["digest"], "oci_digest")
    _pattern(image["reference"], r"[\x21-\x7e]{1,2048}")
    repository, sep, digest = image["reference"].rpartition("@")
    _require(
        bool(repository) and "@" not in repository and sep == "@" and digest == image["digest"]
    )
    bundle = _closed(v["embedded_bundle"], {"object_identity", "sha256", "path"})
    _scalar(bundle["object_identity"], "opaque_ref")
    _scalar(bundle["sha256"], "sha256")
    _require(type(bundle["path"]) is str and bundle["path"] == "/bundles/current")
    platform = _closed(v["platform"], {"os", "architecture", "variant", "runtime_profile"})
    _require(type(platform["os"]) is str and platform["os"] == "linux")
    _require(
        type(platform["architecture"]) is str and platform["architecture"] in ("amd64", "arm64")
    )
    _require(platform["variant"] is None)
    _scalar(platform["runtime_profile"], "opaque_ref")
    _object(v["model_credential_ref"], _CREDENTIAL)
    _object(v["history_signing_credential_ref"], _CREDENTIAL)
    _require(v["model_credential_ref"] != v["history_signing_credential_ref"])
    resources = _closed(v["resources"], {"cpu_millicores", "memory_mib"})
    _integer(resources["cpu_millicores"], 1, 64000)
    _integer(resources["memory_mib"], 1, 1048576)
    _object(
        v["network_policy_ref"],
        {"id": "opaque_ref", "revision": "generation", "config_sha256": "sha256"},
    )
    slots = _closed(v["delivery_slots"], set(_DELIVERY_SLOTS))
    _scalar(slots["schema_version"], "version")
    for name, expected in _DELIVERY_SLOTS.items():
        _require(type(slots[name]) is type(expected) and slots[name] == expected)
    env = _environment(v["environment"])
    names = {BootEnv.env_key(field) for field in _STATIC_FIELDS} | _INTERNAL_STATIC.keys()
    _require(env.keys() == names)
    constants = {
        BootEnv.env_key("plugin_dir"): "/bundles/current",
        BootEnv.env_key("fake_model"): "0",
        BootEnv.env_key("api_backend"): "messages",
        BootEnv.env_key("thinking"): "disabled",
        BootEnv.env_key("model_env_key"): "ANTHROPIC_API_KEY",
        BootEnv.env_key("memory_writes"): "0",
        **_INTERNAL_STATIC,
    }
    _require(all(env[key] == content for key, content in constants.items()))
    image_env = _environment(v["image_environment"])
    _require(all(image_env.get(key) == content for key, content in _IMAGE_FIXED.items()))
    _require(image_env.get("PATH", "").startswith("/app/.venv/bin:"))
    for key, content in image_env.items():
        # This protected format forbids all OTel fields, including inherited Env.
        _require(not key.startswith("OTEL_"))
        _require(not is_reserved_boot_env_name(key) or key in env or key in _IMAGE_FIXED)
        if key in env:
            _require(env[key] == content)


@dataclass(frozen=True, slots=True, repr=False)
class Projection:
    """Immutable structural canonical record; never a qualification claim."""

    _canonical_bytes: bytes = field(repr=False)

    @safe_validation(ProjectionInvalid)
    def __post_init__(self) -> None:
        value = _decode(self._canonical_bytes, _MAX_BYTES)
        _validate(value)
        raw = canonical_json(value)
        _require(raw == self._canonical_bytes and len(raw) <= _MAX_BYTES)
        object.__setattr__(self, "_canonical_bytes", raw)

    def __repr__(self) -> str:
        return "<Projection>"

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def digest(self) -> str:
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self._canonical_bytes)
        return value


def parse_projection(raw: bytes) -> Projection:
    """Parse canonical source bytes only. No provisioner selection or authority."""
    return Projection(raw)


@safe_validation(ProjectionInvalid)
def render_static_environment(
    execution: ProtectedExecutionConfig, manifest: Manifest
) -> dict[str, str]:
    """Use actual BootEnv/Budget rendering, never ordinary worker defaults."""
    _require(type(execution) is ProtectedExecutionConfig and type(manifest) is Manifest)
    # Revalidate direct construction of this existing source record.
    profile = parse_execution_config(execution.canonical_bytes, manifest=manifest).as_dict()
    runner = profile["runner"]
    budget = runner["budget"]
    env = BootEnv.render_worker(
        plugin_dir=runner["plugin_dir"],
        session_id="",
        memory_ref="",
        history_ref="",
        budget=Budget(
            max_output_tokens_per_run=budget["max_output_tokens_per_run"],
            max_usd_per_day=float(Decimal(budget["max_usd_per_day"])),
        ),
        bundle_ref=manifest.as_dict()["bundle_digest"]["object_identity"],
        model=runner["model"],
        reviewer_model=runner["reviewer_model"],
        fake_model=False,
        base_url=runner["model_base_url"],
        api_backend=runner["api_backend"],
        thinking=runner["thinking"],
        model_env_key=runner["model_env_key"],
        memory_writes=False,
    )
    for field_name in ("session_id", "memory_ref", "history_ref"):
        del env[BootEnv.env_key(field_name)]
    env[BootEnv.env_key("fake_model")] = "0"
    env[BootEnv.env_key("max_turns")] = str(runner["max_turns"])
    env.update(_INTERNAL_STATIC)
    return env


@safe_validation(ProjectionInvalid)
def validate_projection_binding(
    projection: Projection,
    *,
    execution: ProtectedExecutionConfig,
    manifest: Manifest,
    measurement: ArtifactMeasurement,
) -> dict[str, Any]:
    """Match selected immutable inputs, returning one structural measured row.

    The caller must separately authenticate selection/observations and establish
    qualification and preventive ownership. This function grants none of them.
    fetch_image_digest remains profile-bound but unused by this embedded format.
    """
    _require(
        type(projection) is Projection
        and type(measurement) is ArtifactMeasurement
        and type(manifest) is Manifest
    )
    value, m, measured = projection.as_dict(), manifest.as_dict(), measurement.as_dict()
    profile = parse_execution_config(execution.canonical_bytes, manifest=manifest).as_dict()
    _require(m["substrate"]["kind"] == "kubernetes")
    _require(m["substrate"]["launch_config_sha256"] == projection.digest)
    for key in ("authority_domain_id", "launch_identity"):
        _require(value[key] == m["substrate"][key])
    _require(measured["manifest_digest"] == manifest.digest)
    for key in ("execution_config_digest", "substrate", "guard_identity"):
        _require(measured[key] == m[key])
    _require(
        measured["launch_projection"]
        == {"schema_version": 1, "format": _FORMAT, "sha256": projection.digest}
    )
    _require(value["delivery_slots"] == measured["delivery_slots"])
    _require(value["image"]["digest"] == m["runner_image_digest"])
    _require(value["embedded_bundle"] == {**m["bundle_digest"], "path": "/bundles/current"})
    _require(value["resources"] == profile["launch"]["resources"])
    _require(value["model_credential_ref"] == profile["runner"]["model_credential_ref"])
    _require(
        value["history_signing_credential_ref"]
        == profile["bindings"]["history_signing_credential_ref"]
    )
    _require(value["environment"] == _entries(render_static_environment(execution, manifest)))
    matches = [
        row
        for row in measured["platform_mappings"]
        if all(
            type(row[key]) is type(content) and row[key] == content
            for key, content in value["platform"].items()
        )
    ]
    _require(len(matches) == 1)
    row: dict[str, Any] = matches[0]
    _require(
        row["oci_digest"] == value["image"]["digest"]
        and row["pinned_image_reference"] == value["image"]["reference"]
    )
    return dict(row)


@safe_validation(ProjectionInvalid)
def render_delivery_environment(
    *, execution: ProtectedExecutionConfig, manifest: Manifest, delivery: Dispatchable
) -> dict[str, str]:
    """Deterministic nonsecret slots from independently verified delivery."""
    _require(type(delivery) is Dispatchable)
    profile = parse_execution_config(execution.canonical_bytes, manifest=manifest).as_dict()
    envelope, m = delivery.envelope.as_dict(), manifest.as_dict()
    _require(envelope["manifest_digest"] == manifest.digest)
    for key in (
        "runtime_id",
        "runtime_generation",
        "qualification_id",
        "runner_image_digest",
        "execution_config_digest",
    ):
        _require(envelope[key] == m[key])
    _require(envelope["bundle_digest"] == m["bundle_digest"]["sha256"])
    _require(delivery.intent.as_dict()["identity"]["agent_id"] == profile["agent_id"])
    history_key = protected_history_key(delivery)
    agent = profile["agent_id"]
    base = profile["bindings"]["runner_facing_api_base_url"].rstrip("/")
    return {
        BootEnv.env_key("session_id"): f"agent-{agent}-thread-{history_key}",
        BootEnv.env_key(
            "history_ref"
        ): f"{base}/agents/{agent}/state/transcript/{quote(history_key, safe='')}",
    }


@safe_validation(ProjectionInvalid)
def render_pod(
    projection: Projection,
    *,
    launch_id: str,
    pod_name: str,
    secret_name: str,
    session_id: str,
    history_ref: str,
) -> dict[str, Any]:
    """Render source-only exact Pod request; never mint or certify assignments.

    The caller must recompute session/history with render_delivery_environment.
    Literal slots are not authorization, and SecretKeyRefs are not assignment proof.
    """
    _require(type(projection) is Projection)
    _scalar(launch_id, "uuid")
    for name in (pod_name, secret_name):
        _pattern(name, r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
    _text(session_id, 8192)
    _text(history_ref, 8192)
    value = projection.as_dict()
    env = _environment(value["image_environment"])
    env.update(_environment(value["environment"]))
    env.update(
        {
            BootEnv.env_key("session_id"): session_id,
            BootEnv.env_key("history_ref"): history_ref,
            BootEnv.env_key("sandbox_id"): pod_name,
        }
    )
    environment: list[dict[str, Any]] = list(_entries(env))
    for name, key in (
        (BootEnv.env_key("runner_token"), "runner-token"),
        (BootEnv.env_key("history_token"), "history-token"),
        ("ANTHROPIC_API_KEY", "model-credential"),
    ):
        environment.append(
            {
                "name": name,
                "valueFrom": {"secretKeyRef": {"name": secret_name, "key": key, "optional": False}},
            }
        )
    environment.sort(key=lambda item: item["name"])
    memory = f"{value['resources']['memory_mib']}Mi"
    resources = {"cpu": f"{value['resources']['cpu_millicores']}m", "memory": memory}
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": value["namespace"],
            "labels": {_LAUNCH_LABEL: launch_id},
            "finalizers": [_LAUNCH_LABEL],
        },
        "spec": {
            "containers": [
                {
                    "name": "runner",
                    "image": value["image"]["reference"],
                    "imagePullPolicy": "Always",
                    "command": ["/app/.venv/bin/python", "-m", "curie_runner"],
                    "args": [],
                    "workingDir": "/app",
                    "env": environment,
                    "securityContext": {
                        "privileged": False,
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {"requests": dict(resources), "limits": dict(resources)},
                    "volumeMounts": [
                        {
                            "name": name,
                            "mountPath": path,
                            "readOnly": False,
                            "mountPropagation": "None",
                        }
                        for name, path in (
                            ("tmp", "/tmp"),
                            ("home", "/home/runner"),
                            ("scratch", "/scratch"),
                        )
                    ],
                    "ports": [{"name": "http", "containerPort": 8080, "protocol": "TCP"}],
                    "terminationMessagePath": "/dev/termination-log",
                    "terminationMessagePolicy": "File",
                    "stdin": False,
                    "stdinOnce": False,
                    "tty": False,
                }
            ],
            "volumes": [
                {"name": name, "emptyDir": {"medium": "Memory", "sizeLimit": memory}}
                for name in ("tmp", "home", "scratch")
            ],
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "hostNetwork": False,
            "hostPID": False,
            "hostIPC": False,
            "dnsPolicy": "ClusterFirst",
            "terminationGracePeriodSeconds": 5,
            "serviceAccountName": value["service_account"],
            "runtimeClassName": value["runtime_class"],
            "schedulerName": "default-scheduler",
            "nodeSelector": {
                "kubernetes.io/os": "linux",
                "kubernetes.io/arch": value["platform"]["architecture"],
            },
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 1000,
                "runAsGroup": 1000,
                "fsGroup": 1000,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "tolerations": [
                {
                    "key": "node.kubernetes.io/" + key,
                    "operator": "Exists",
                    "effect": "NoExecute",
                    "tolerationSeconds": 300,
                    "value": "",
                }
                for key in ("not-ready", "unreachable")
            ],
        },
    }


def _exact(actual: Any, expected: Any) -> None:
    """Type-exact recursive match: bool is never an integer default."""
    _require(type(actual) is type(expected))
    if type(expected) is dict:
        _require(actual.keys() == expected.keys())
        for key in expected:
            _exact(actual[key], expected[key])
    elif type(expected) is list:
        _require(len(actual) == len(expected))
        for left, right in zip(actual, expected, strict=True):
            _exact(left, right)
    else:
        _require(actual == expected)


def _defaults(spec: dict[str, Any]) -> None:
    """Only explicitly named serialization defaults; never discard unknowns."""
    if "serviceAccount" in spec:
        _require(spec.pop("serviceAccount") == spec.get("serviceAccountName"))
    for field_name in _EMPTY_SPEC:
        if field_name in spec:
            _require(type(spec[field_name]) is list and spec.pop(field_name) == [])
    # The Priority admission plugin adds these when no priorityClassName is set.
    for field_name, content in (("priority", 0), ("preemptionPolicy", "PreemptLowerPriority")):
        if field_name in spec:
            _require(type(spec[field_name]) is type(content) and spec.pop(field_name) == content)
    for field_name, content in (
        ("hostNetwork", False),
        ("hostPID", False),
        ("hostIPC", False),
        ("dnsPolicy", "ClusterFirst"),
        ("schedulerName", "default-scheduler"),
    ):
        spec.setdefault(field_name, content)
    containers: Any = spec.get("containers")
    _require(type(containers) is list and len(containers) == 1 and type(containers[0]) is dict)
    container = containers[0]
    for field_name in _EMPTY_CONTAINER:
        if field_name in container:
            _require(type(container[field_name]) is list and container.pop(field_name) == [])
    container.setdefault("args", [])
    for field_name, content in (
        ("terminationMessagePath", "/dev/termination-log"),
        ("terminationMessagePolicy", "File"),
        ("stdin", False),
        ("stdinOnce", False),
        ("tty", False),
    ):
        container.setdefault(field_name, content)
    mounts: Any = container.get("volumeMounts")
    _require(type(mounts) is list)
    for mount in mounts:
        _require(type(mount) is dict)
        mount.setdefault("readOnly", False)
        mount.setdefault("mountPropagation", "None")
    tolerations: Any = spec.get("tolerations")
    _require(type(tolerations) is list and len(tolerations) == 2)
    for toleration in tolerations:
        _require(type(toleration) is dict and type(toleration.get("key")) is str)
        toleration.setdefault("value", "")
    tolerations.sort(key=lambda item: item["key"])


@safe_validation(ProjectionInvalid)
def extract_pod_projection(
    pod: object,
    *,
    projection: Projection,
    launch_id: str,
    pod_name: str,
    pod_uid: str,
    secret_name: str,
    session_id: str,
    history_ref: str,
) -> Projection:
    """Match exact admitted static shape; no API authentication/factual authority.

    UID/name is ownership comparison only. Actual Secret bytes, UID/resourceVersion
    assignment, Node/container/platform/endpoint observations remain separate proof.
    Pod status and managed fields can never suppress a static spec check.
    """
    _require(type(pod) is dict)
    raw = canonical_json(pod)
    _require(len(raw) <= _OBSERVATION_CAP)
    observed = _decode(raw, _OBSERVATION_CAP)
    _require(observed.keys() <= {"apiVersion", "kind", "metadata", "spec", "status"})
    _require({"apiVersion", "kind", "metadata", "spec"} <= observed.keys())
    _require(observed["apiVersion"] == "v1" and observed["kind"] == "Pod")
    expected = render_pod(
        projection,
        launch_id=launch_id,
        pod_name=pod_name,
        secret_name=secret_name,
        session_id=session_id,
        history_ref=history_ref,
    )
    metadata = observed["metadata"]
    _require(type(metadata) is dict and metadata.keys() <= _METADATA)
    _scalar(pod_uid, "uuid")
    for key in ("name", "namespace", "labels", "finalizers"):
        _exact(metadata.get(key), expected["metadata"][key])
    _require(metadata.get("uid") == pod_uid)
    _text(metadata.get("resourceVersion"), 2048)
    for key in ("deletionTimestamp", "deletionGracePeriodSeconds"):
        _require(metadata.get(key) is None)
    if "creationTimestamp" in metadata:
        _timestamp(metadata["creationTimestamp"])
    if "generation" in metadata:
        _integer(metadata["generation"], 1, 9223372036854775807)
    if "ownerReferences" in metadata:
        _require(type(metadata["ownerReferences"]) is list and metadata["ownerReferences"] == [])
    if "managedFields" in metadata:
        _require(type(metadata["managedFields"]) is list)
        for entry in metadata["managedFields"]:
            _require(type(entry) is dict and entry.keys() <= _MANAGED_FIELDS)
            for key in entry.keys() - {"fieldsV1", "time"}:
                _text(entry[key])
            if "time" in entry:
                _timestamp(entry["time"])
            if "fieldsV1" in entry:
                _require(type(entry["fieldsV1"]) is dict)
    spec = observed["spec"]
    _require(type(spec) is dict)
    if "nodeName" in spec:
        _text(spec.pop("nodeName"), 253, empty=True)
    if "status" in observed:
        _require(type(observed["status"]) is dict)
    _defaults(spec)
    _exact(spec, expected["spec"])
    return Projection(projection.canonical_bytes)


class ProjectionDirectory:
    """Explicit content-addressed local loader. Mount ownership is separate proof."""

    __slots__ = ("_fd",)

    @safe_validation(ProjectionInvalid)
    def __init__(self, root: str) -> None:
        self._fd = _open_root(root)

    def __repr__(self) -> str:
        return "ProjectionDirectory()"

    @safe_validation(ProjectionInvalid)
    def __enter__(self) -> ProjectionDirectory:
        _require(self._fd >= 0)
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @safe_validation(ProjectionInvalid)
    def close(self) -> None:
        if self._fd >= 0:
            fd, self._fd = self._fd, -1
            os.close(fd)

    @safe_validation(ProjectionInvalid)
    def load(
        self,
        *,
        manifest: Manifest,
        measurement: ArtifactMeasurement,
        execution: ProtectedExecutionConfig,
    ) -> Projection:
        _require(type(measurement) is ArtifactMeasurement and self._fd >= 0)
        selection = measurement.as_dict()["launch_projection"]
        _require(selection["format"] == _FORMAT)
        digest = selection["sha256"]
        _scalar(digest, "sha256")
        filename = digest + ".json"
        fd = -1
        try:
            fd = os.open(
                filename,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                dir_fd=self._fd,
            )
            before = os.fstat(fd)
            _require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= _MAX_BYTES)
            chunks: list[bytes] = []
            size = 0
            while size <= _MAX_BYTES:
                chunk = os.read(fd, min(65536, _MAX_BYTES + 1 - size))
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
            _require(size <= _MAX_BYTES)
            after = os.fstat(fd)
            current = os.stat(filename, dir_fd=self._fd, follow_symlinks=False)
            _require(
                stat.S_ISREG(current.st_mode)
                and _fingerprint(before) == _fingerprint(after) == _fingerprint(current)
            )
            projection = parse_projection(b"".join(chunks))
            _require(projection.digest == digest)
            validate_projection_binding(
                projection, manifest=manifest, execution=execution, measurement=measurement
            )
            return projection
        finally:
            if fd >= 0:
                os.close(fd)
