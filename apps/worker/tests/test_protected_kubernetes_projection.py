"""Synthetic closed projection vectors; no deployment qualification."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
from copy import deepcopy

import pytest

from .protected_configuration_vectors import canonical, execution_profile

LAUNCH = "77777777-7777-4777-8777-777777777777"
UID = "88888888-8888-4888-8888-888888888888"
SLOTS = {
    "schema_version": 1,
    "execution_identity": "verified-delivery-v1",
    "history_binding": "protected-history-v1",
    "runner_token": "fresh-owned-launch-v1",
    "history_token": "scoped-history-credential-v1",
    "workload_identity": "control-plane-cold-create-v1",
    "dial_identity": "owned-workload-endpoint-v1",
}


def product():
    name = "curie_worker.protected_kubernetes_projection"
    assert importlib.util.find_spec(name) is not None, "closed projection is absent"
    return importlib.import_module(name)


def static_environment():
    return {
        "CURIE_PLUGIN_DIR": "/bundles/current",
        "CURIE_BUDGET": (
            '{"max_output_tokens_per_run":4096,"task_budget_hint":null,"max_usd_per_day":10.25}'
        ),
        "CURIE_BUNDLE_REF": "bundle/example-v1",
        "CURIE_MODEL": "example-model",
        "CURIE_REVIEWER_MODEL": "example-reviewer",
        "CURIE_FAKE_MODEL": "0",
        "ANTHROPIC_BASE_URL": "https://model.example.com",
        "CURIE_MODEL_API_BACKEND": "messages",
        "CURIE_THINKING": "disabled",
        "CURIE_MODEL_ENV_KEY": "ANTHROPIC_API_KEY",
        "CURIE_MAX_TURNS": "8",
        "CURIE_MEMORY_WRITES": "0",
        "CURIE_HARNESS": "claude-agent-sdk",
        "CURIE_FALSE_COMPLETION_CHECK": "0",
    }


def entries(value):
    return [{"name": k, "value": v} for k, v in sorted(value.items())]


def projection_value():
    profile = execution_profile()
    return {
        "schema_version": 1,
        "format": "kubernetes-cold-pod-v1",
        "authority_domain_id": profile["launch"]["substrate"]["authority_domain_id"],
        "launch_identity": "launch/example-v1",
        "namespace": "acme-runtime",
        "service_account": "acme-runner",
        "runtime_class": "acme-runtime",
        "image": {
            "reference": "registry.example.com/acme-runner@sha256:" + "e" * 64,
            "digest": "sha256:" + "e" * 64,
        },
        "embedded_bundle": {
            "object_identity": "bundle/example-v1",
            "sha256": "f" * 64,
            "path": "/bundles/current",
        },
        "platform": {
            "os": "linux",
            "architecture": "amd64",
            "variant": None,
            "runtime_profile": "runtime/example",
        },
        "image_environment": entries(
            {
                "HOME": "/home/runner",
                "CURIE_RUNNER_PORT": "8080",
                "CURIE_PROC_DUMPABLE_PRELOAD": "/usr/local/lib/curie/libproc_dumpable.so",
                "PATH": "/app/.venv/bin:/measured/bin",
                "LANG": "C.UTF-8",
            }
        ),
        "environment": entries(static_environment()),
        "model_credential_ref": profile["runner"]["model_credential_ref"],
        "history_signing_credential_ref": profile["bindings"]["history_signing_credential_ref"],
        "resources": profile["launch"]["resources"],
        "network_policy_ref": {"id": "network/example", "revision": "1", "config_sha256": "a" * 64},
        "delivery_slots": dict(SLOTS),
    }


def pod_expected():
    value = projection_value()
    env = {x["name"]: x["value"] for x in value["image_environment"] + value["environment"]}
    env.update(
        CURIE_SESSION_ID="session/example",
        CURIE_HISTORY_REF="https://api.example.com/history",
        CURIE_SANDBOX_ID="acme-pod",
    )
    env_list = entries(env)
    for name, key in [
        ("CURIE_RUNNER_TOKEN", "runner-token"),
        ("CURIE_HISTORY_TOKEN", "history-token"),
        ("ANTHROPIC_API_KEY", "model-credential"),
    ]:
        env_list.append(
            {
                "name": name,
                "valueFrom": {
                    "secretKeyRef": {"name": "acme-secret", "key": key, "optional": False}
                },
            }
        )
    env_list.sort(key=lambda e: e["name"])
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "acme-pod",
            "namespace": "acme-runtime",
            "labels": {"curietech.ai/protected-launch": LAUNCH},
            "finalizers": ["curietech.ai/protected-launch"],
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
                    "env": env_list,
                    "securityContext": {
                        "privileged": False,
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "512Mi"},
                        "limits": {"cpu": "500m", "memory": "512Mi"},
                    },
                    "volumeMounts": [
                        {"name": n, "mountPath": p, "readOnly": False, "mountPropagation": "None"}
                        for n, p in [
                            ("tmp", "/tmp"),
                            ("home", "/home/runner"),
                            ("scratch", "/scratch"),
                        ]
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
                {"name": n, "emptyDir": {"medium": "Memory", "sizeLimit": "512Mi"}}
                for n in ("tmp", "home", "scratch")
            ],
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "hostNetwork": False,
            "hostPID": False,
            "hostIPC": False,
            "dnsPolicy": "ClusterFirst",
            "terminationGracePeriodSeconds": 5,
            "serviceAccountName": "acme-runner",
            "runtimeClassName": "acme-runtime",
            "schedulerName": "default-scheduler",
            "nodeSelector": {"kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64"},
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


def kwargs():
    return dict(
        launch_id=LAUNCH,
        pod_name="acme-pod",
        secret_name="acme-secret",
        session_id="session/example",
        history_ref="https://api.example.com/history",
    )


def observation():
    pod = pod_expected()
    pod["metadata"].update(uid=UID, resourceVersion="9", creationTimestamp="2026-10-12T00:00:00Z")
    pod["spec"]["nodeName"] = "acme-node"
    pod["status"] = {}
    return pod


def test_canonical_projection_is_immutable_redacted_and_copies():
    p = product()
    value = projection_value()
    parsed = p.parse_projection(canonical(value))
    assert parsed.canonical_bytes == canonical(value)
    assert parsed.digest == hashlib.sha256(canonical(value)).hexdigest()
    copy = parsed.as_dict()
    copy["model_credential_ref"]["generation"] = "2"
    assert parsed.as_dict() == value
    assert "acme" not in repr(parsed)
    with pytest.raises((AttributeError, TypeError)):
        parsed._canonical_bytes = b"{}"


def test_renderer_matches_independent_complete_pod():
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    assert p.render_pod(projection, **kwargs()) == pod_expected()
    extracted = p.extract_pod_projection(
        observation(), projection=projection, pod_uid=UID, **kwargs()
    )
    assert extracted.canonical_bytes == projection.canonical_bytes


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update(unknown="private-marker"),
        lambda p: p["image"].update(unknown="private-marker"),
        lambda p: p["platform"].update(variant="v8"),
        lambda p: p["resources"].update(cpu_millicores=True),
        lambda p: p["environment"].append({"name": "CURIE_STATE_TOKEN", "value": "private-marker"}),
        lambda p: p["image_environment"].append(
            {"name": "ANTHROPIC_API_KEY", "value": "private-marker"}
        ),
        lambda p: p["delivery_slots"].update(runner_token="custom"),
        lambda p: p.update(model_credential_ref=p["history_signing_credential_ref"]),
    ],
)
def test_projection_rejects_closed_semantic_mutations(mutate):
    p = product()
    value = projection_value()
    mutate(value)
    with pytest.raises(p.ProjectionInvalid):
        p.parse_projection(canonical(value))


@pytest.mark.parametrize(
    "raw",
    [
        b"{}",
        b' {"private-marker":1}',
        b"\xff",
        b'{"schema_version":1,"schema_version":1}',
        b"[]",
        b"null",
        b" " * 262145,
    ],
)
def test_projection_bad_bytes_detach_all_exception_context(raw):
    p = product()
    try:
        raise OSError("private-path-marker")
    except OSError:
        with pytest.raises(p.ProjectionInvalid) as caught:
            p.parse_projection(raw)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert "private" not in str(caught.value)


@pytest.mark.parametrize(
    "path,bad",
    [
        (("spec", "hostNetwork"), True),
        (("spec", "overhead"), {"cpu": "1m"}),
        (("spec", "initContainers"), [{"name": "extra"}]),
        (("spec", "imagePullSecrets"), [{"name": "extra"}]),
        (("metadata", "annotations"), {}),
        (("metadata", "ownerReferences"), [{"uid": UID}]),
        (("metadata", "finalizers"), []),
        (("spec", "tolerations"), []),
        (("spec", "serviceAccount"), "other"),
        (("spec", "hostPID"), 0),
    ],
)
def test_extractor_refuses_unsupported_authority_fields(path, bad):
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    target = pod
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = bad
    with pytest.raises(p.ProjectionInvalid):
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())


def test_extractor_normalizes_only_named_defaults_and_detaches_mutable_input():
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    pod["spec"]["serviceAccount"] = "acme-runner"
    pod["spec"]["tolerations"].reverse()
    del pod["spec"]["containers"][0]["stdin"]
    del pod["spec"]["tolerations"][0]["value"]
    original = deepcopy(pod)
    assert (
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs()) == projection
    )
    assert pod == original


def binding_vectors():
    """Independent immutable source vectors, with no runtime observations/qualification."""
    from curie_protected_hooks.artifact_measurement import parse_artifact_measurement
    from curie_worker.protected_configuration import parse_execution_config

    from .protected_configuration_vectors import manifest_for

    value = projection_value()
    profile = execution_profile()
    profile["launch"]["substrate"]["kind"] = "kubernetes"
    profile["launch"]["substrate"]["launch_config_sha256"] = hashlib.sha256(
        canonical(value)
    ).hexdigest()
    manifest = manifest_for(profile)
    execution = parse_execution_config(canonical(profile), manifest=manifest)
    m = manifest.as_dict()
    row = dict(
        value["platform"],
        pinned_image_reference=value["image"]["reference"],
        oci_digest=value["image"]["digest"],
        observed_image_id="containerd://sha256:" + "e" * 64,
        image_config_id="sha256:" + "1" * 64,
    )
    measured = dict(
        schema_version=1,
        kind="protected-artifact-qualification",
        manifest_digest=manifest.digest,
        execution_config_digest=execution.digest,
        substrate=m["substrate"],
        guard_identity=m["guard_identity"],
        platform_mappings=[row],
        launch_projection=dict(
            schema_version=1,
            format="kubernetes-cold-pod-v1",
            sha256=hashlib.sha256(canonical(value)).hexdigest(),
        ),
        delivery_slots=dict(SLOTS),
        enforcement_evidence_ref="sha256:" + "a" * 64,
    )
    return value, execution, manifest, parse_artifact_measurement(canonical(measured)), row


def test_binding_uses_actual_bootenv_budget_and_complete_measured_path():
    p = product()
    value, execution, manifest, measurement, row = binding_vectors()
    projection = p.parse_projection(canonical(value))
    assert p.render_static_environment(execution, manifest) == static_environment()
    assert (
        p.validate_projection_binding(
            projection, execution=execution, manifest=manifest, measurement=measurement
        )
        == row
    )
    pod = p.render_pod(projection, **kwargs())
    rendered = {e["name"]: e["value"] for e in pod["spec"]["containers"][0]["env"] if "value" in e}
    assert rendered["PATH"] == "/app/.venv/bin:/measured/bin"
    assert rendered["LANG"] == "C.UTF-8"
    assert rendered["CURIE_PROC_DUMPABLE_PRELOAD"] == "/usr/local/lib/curie/libproc_dumpable.so"
    assert (
        not {
            "CURIE_MEMORY_REF",
            "EXAMPLE_PRIVATE_CREDENTIAL_SOURCE",
            "CURIE_STATE_TOKEN",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
        }
        & rendered.keys()
    )


@pytest.mark.parametrize(
    "path,bad",
    [
        (("resources", "memory_mib"), 513),
        (("model_credential_ref", "generation"), "2"),
        (("history_signing_credential_ref", "generation"), "2"),
        (("image", "reference"), "other.example.com/runner@sha256:" + "e" * 64),
        (("embedded_bundle", "object_identity"), "bundle/other"),
        (("authority_domain_id",), "99999999-9999-4999-8999-999999999999"),
        (("launch_identity",), "launch/other"),
        (("platform", "architecture"), "arm64"),
    ],
)
def test_structurally_valid_substitution_fails_distinct_binding_check(path, bad):
    p = product()
    value, execution, manifest, measurement, _ = binding_vectors()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = bad
    projection = p.parse_projection(canonical(value))
    with pytest.raises(p.ProjectionInvalid):
        p.validate_projection_binding(
            projection, execution=execution, manifest=manifest, measurement=measurement
        )


def test_profile_static_environment_substitution_needs_bound_profile():
    p = product()
    value, execution, manifest, measurement, _ = binding_vectors()
    next(e for e in value["environment"] if e["name"] == "CURIE_MODEL")["value"] = (
        "example-other-model"
    )
    projection = p.parse_projection(canonical(value))
    with pytest.raises(p.ProjectionInvalid):
        p.validate_projection_binding(
            projection, execution=execution, manifest=manifest, measurement=measurement
        )


def test_measurement_and_profile_mutations_do_not_gain_binding():
    from curie_protected_hooks.artifact_measurement import parse_artifact_measurement
    from curie_worker.protected_configuration import ProtectedExecutionConfig

    p = product()
    value, execution, manifest, measurement, _ = binding_vectors()
    projection = p.parse_projection(canonical(value))
    changed = measurement.as_dict()
    changed["platform_mappings"][0]["pinned_image_reference"] = (
        "other.example.com/runner@sha256:" + "e" * 64
    )
    with pytest.raises(p.ProjectionInvalid):
        p.validate_projection_binding(
            projection,
            execution=execution,
            manifest=manifest,
            measurement=parse_artifact_measurement(canonical(changed)),
        )
    changed = execution.as_dict()
    changed["runner"]["model"] = "example-other-model"
    with pytest.raises(p.ProjectionInvalid):
        p.render_static_environment(ProtectedExecutionConfig(canonical(changed)), manifest)


def test_delivery_dynamic_slots_use_exact_existing_protected_history():
    from dataclasses import replace

    from curie_protected_hooks.admission_records import parse_envelope, parse_intent

    from .test_protected_history import dispatchable

    p = product()
    _, execution, manifest, _, _ = binding_vectors()
    delivery = dispatchable()
    envelope = delivery.envelope.as_dict()
    envelope.update(manifest_digest=manifest.digest, execution_config_digest=execution.digest)
    intent = delivery.intent.as_dict()
    intent.update(
        manifest_digest=manifest.digest,
        envelope_sha256=hashlib.sha256(canonical(envelope)).hexdigest(),
    )
    delivery = replace(
        delivery,
        envelope=parse_envelope(canonical(envelope)),
        intent=parse_intent(canonical(intent)),
    )
    env = p.render_delivery_environment(execution=execution, manifest=manifest, delivery=delivery)
    assert env["CURIE_SESSION_ID"].startswith(
        "agent-11111111-1111-4111-8111-111111111111-thread-%40protected:"
    )
    assert env["CURIE_HISTORY_REF"].startswith(
        "https://api.example.com/agents/11111111-1111-4111-8111-111111111111/state/transcript/%2540protected%3A"
    )
    assert env.keys() == {"CURIE_SESSION_ID", "CURIE_HISTORY_REF"}
    with pytest.raises(p.ProjectionInvalid):
        p.render_delivery_environment(
            execution=execution, manifest=manifest, delivery=dispatchable()
        )


def authority_dictionary_paths(value, prefix=()):
    if isinstance(value, dict):
        yield prefix
        for key, item in value.items():
            yield from authority_dictionary_paths(item, prefix + (key,))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from authority_dictionary_paths(item, prefix + (index,))


@pytest.mark.parametrize(
    "path", list(authority_dictionary_paths(pod_expected()["spec"], ("spec",)))
)
def test_every_static_pod_dictionary_refuses_unknown_member(path):
    # Provider shape is the Kubernetes v1 Pod API, qualified defaults are spec-bound:
    # https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    target = pod
    for key in path:
        target = target[key]
    target["unsupportedAuthority"] = "private-marker"
    with pytest.raises(p.ProjectionInvalid):
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda pod: pod["spec"]["containers"][0]["env"][0]["valueFrom"]["secretKeyRef"].update(
            optional=True
        ),
        lambda pod: pod["spec"]["containers"][0]["env"][0]["valueFrom"]["secretKeyRef"].update(
            name="other-secret"
        ),
        lambda pod: pod["spec"]["containers"][0]["env"][0]["valueFrom"]["secretKeyRef"].update(
            key="runner-token"
        ),
        lambda pod: pod["spec"]["containers"][0]["env"].append(
            {"name": "CURIE_SESSION_ID", "value": "forged"}
        ),
        lambda pod: pod["spec"]["tolerations"][0].update(tolerationSeconds=True),
        lambda pod: pod["spec"]["tolerations"][0].update(tolerationSeconds=301),
        lambda pod: pod["spec"]["tolerations"].__setitem__(1, pod["spec"]["tolerations"][0]),
        lambda pod: pod["metadata"].update(generation=True),
        lambda pod: pod["metadata"].update(creationTimestamp="2026-02-30T00:00:00Z"),
        lambda pod: pod["metadata"].update(deletionTimestamp="2026-10-12T00:00:00Z"),
        lambda pod: pod["metadata"].update(managedFields=[{"unknown": "private-marker"}]),
        lambda pod: pod["metadata"].update(ownerReferences=None),
    ],
)
def test_assignment_defaults_and_metadata_mutations_refuse(mutate):
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    mutate(pod)
    with pytest.raises(p.ProjectionInvalid):
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())


def test_control_plane_bookkeeping_cannot_change_the_projection():
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    pod["metadata"].update(
        ownerReferences=[],
        deletionTimestamp=None,
        generation=1,
        managedFields=[
            {
                "manager": "provider",
                "operation": "Update",
                "apiVersion": "v1",
                "time": "2026-10-12T00:00:00Z",
                "fieldsType": "FieldsV1",
                "fieldsV1": {"f:spec": {}},
            }
        ],
    )
    assert (
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs()) == projection
    )
    pod["spec"]["containers"][0]["command"] = ["/bin/other"]
    with pytest.raises(p.ProjectionInvalid):
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())


def test_projection_directory_selects_only_digest_and_detaches_missing_path(tmp_path):
    p = product()
    value, execution, manifest, measurement, _ = binding_vectors()
    raw = canonical(value)
    selected = tmp_path / (hashlib.sha256(raw).hexdigest() + ".json")
    selected.write_bytes(raw)
    with p.ProjectionDirectory(str(tmp_path)) as directory:
        assert (
            directory.load(
                execution=execution, manifest=manifest, measurement=measurement
            ).canonical_bytes
            == raw
        )
        selected.unlink()
        with pytest.raises(p.ProjectionInvalid) as caught:
            directory.load(execution=execution, manifest=manifest, measurement=measurement)
        assert caught.value.__context__ is None and caught.value.__cause__ is None
        assert str(tmp_path) not in str(caught.value)
    with pytest.raises(p.ProjectionInvalid):
        directory.load(execution=execution, manifest=manifest, measurement=measurement)


def test_projection_loader_refuses_symlinks_replaced_bytes_and_noncanonical_json(tmp_path):
    p = product()
    value, execution, manifest, measurement, _ = binding_vectors()
    raw = canonical(value)
    other = tmp_path / "other.json"
    other.write_bytes(raw)
    selected = tmp_path / (hashlib.sha256(raw).hexdigest() + ".json")
    selected.symlink_to(other)
    with p.ProjectionDirectory(str(tmp_path)) as directory:
        with pytest.raises(p.ProjectionInvalid):
            directory.load(execution=execution, manifest=manifest, measurement=measurement)
        selected.unlink()
        selected.write_bytes(raw + b"\n")
        with pytest.raises(p.ProjectionInvalid):
            directory.load(execution=execution, manifest=manifest, measurement=measurement)


@pytest.mark.parametrize("name", ["CURIE_SESSION_ID", "CURIE_HISTORY_REF", "CURIE_SANDBOX_ID"])
def test_delivery_literal_slot_cannot_be_substituted_in_admitted_pod(name):
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    next(e for e in pod["spec"]["containers"][0]["env"] if e["name"] == name)["value"] = (
        "substitution"
    )
    with pytest.raises(p.ProjectionInvalid):
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())


@pytest.mark.parametrize("path", list(authority_dictionary_paths(projection_value())))
def test_every_projection_dictionary_refuses_unknown_member(path):
    p = product()
    value = projection_value()
    target = value
    for key in path:
        target = target[key]
    target["unknownAuthority"] = "private-marker"
    with pytest.raises(p.ProjectionInvalid):
        p.parse_projection(canonical(value))


@pytest.mark.parametrize(
    "case",
    ["order", "duplicate", "nul", "long", "missing-home", "bad-path", "conflict", "service-env"],
)
def test_complete_image_and_static_environment_reject_ambiguity(case):
    p = product()
    value = projection_value()
    if case == "order":
        value["environment"].reverse()
    elif case == "duplicate":
        value["environment"].insert(0, value["environment"][0])
    elif case == "nul":
        value["image_environment"][0]["value"] = "x\0y"
    elif case == "long":
        value["image_environment"][0]["value"] = "é" * 4097
    elif case == "missing-home":
        value["image_environment"] = [e for e in value["image_environment"] if e["name"] != "HOME"]
    elif case == "bad-path":
        next(e for e in value["image_environment"] if e["name"] == "PATH")["value"] = "/other/bin"
    elif case == "conflict":
        value["image_environment"] = entries(
            {e["name"]: e["value"] for e in value["image_environment"]} | {"CURIE_MODEL": "another"}
        )
    else:
        value["image_environment"] = entries(
            {e["name"]: e["value"] for e in value["image_environment"]}
            | {"CURIE_SESSION_ID": "delivery"}
        )
    with pytest.raises(p.ProjectionInvalid):
        p.parse_projection(canonical(value))


def test_canonical_projection_digest_vector_and_noncanonical_refusal():
    p = product()
    raw = canonical(projection_value())
    assert (
        p.parse_projection(raw).digest
        == "44593bb985d2bb043270a2b9a5fdd45328574099face367f37e573cd2c8c30a1"
    )
    with pytest.raises(p.ProjectionInvalid):
        p.parse_projection(raw + b"\n")
    duplicate = raw.replace(b'"schema_version":1', b'"schema_version":1,"schema_version":1', 1)
    with pytest.raises(p.ProjectionInvalid) as caught:
        p.parse_projection(duplicate)
    assert caught.value.__context__ is None and caught.value.__cause__ is None


def test_optional_forbidden_collections_accept_empty_but_never_null():
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    pod["spec"].update(
        initContainers=[],
        ephemeralContainers=[],
        imagePullSecrets=[],
        schedulingGates=[],
        topologySpreadConstraints=[],
    )
    pod["spec"]["containers"][0].update(envFrom=[], volumeDevices=[])
    assert (
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs()) == projection
    )
    pod["spec"]["imagePullSecrets"] = None
    with pytest.raises(p.ProjectionInvalid):
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())


OTEL_IMAGE_ENV_NAMES = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
    "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
    "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
    "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
    "OTEL_SERVICE_NAME",
    "OTEL_FUTURE_SETTING",
)


@pytest.mark.parametrize("name", OTEL_IMAGE_ENV_NAMES)
def test_otel_image_environment_cannot_enter_parse_render_extract_pipeline(name):
    # The format excludes every OTel field, including inherited image settings.
    # Removing the image OTel refusal would admit and reproduce this whole vector.
    p = product()
    value = projection_value()
    value["image_environment"] = entries(
        {e["name"]: e["value"] for e in value["image_environment"]}
        | {name: "synthetic-setting"}
    )
    raw = canonical(value)
    with pytest.raises(p.ProjectionInvalid):
        projection = p.parse_projection(raw)
        pod = p.render_pod(projection, **kwargs())
        assert {"name": name, "value": "synthetic-setting"} in pod["spec"]["containers"][0]["env"]
        pod["metadata"].update(uid=UID, resourceVersion="1")
        assert (
            p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())
            .canonical_bytes
            == raw
        )


@pytest.mark.parametrize("name", OTEL_IMAGE_ENV_NAMES)
def test_otel_injected_into_observed_pod_refuses_extraction(name):
    p = product()
    projection = p.parse_projection(canonical(projection_value()))
    pod = observation()
    pod["spec"]["containers"][0]["env"].append(
        {"name": name, "value": "synthetic-setting"}
    )
    with pytest.raises(p.ProjectionInvalid):
        p.extract_pod_projection(pod, projection=projection, pod_uid=UID, **kwargs())
