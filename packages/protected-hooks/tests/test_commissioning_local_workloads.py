"""Production API ingress objects of a local installation, @spec PROTECTED-COMMISSIONING-6.

Pure rendering tests; the cluster test installs the same objects.
"""

from __future__ import annotations

import base64
import importlib
import importlib.util
import json
from typing import Any

import pytest

OWNER = "curie-commission-0123456789ab"
NAMESPACE = OWNER + "-protected"
API_IMAGE = "localhost:5000/" + OWNER + "/api@sha256:" + "a" * 64


def module() -> Any:
    name = "curie_protected_hooks.commissioning_local_workloads"
    assert importlib.util.find_spec(name) is not None, "local workloads not implemented"
    return importlib.import_module(name)


def rendered() -> tuple[Any, list[dict[str, Any]]]:
    api = module()
    secrets = api.ServiceSecrets.fresh()
    objects = [
        *api.service_objects(OWNER, NAMESPACE, secrets),
        api.migration_pod(OWNER, NAMESPACE, API_IMAGE),
        api.runtime_secret(OWNER, NAMESPACE, {"manifest.json": b"{}"}),
        *api.api_objects(OWNER, NAMESPACE, API_IMAGE),
    ]
    return secrets, objects


def _pod_specs(objects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs = []
    for item in objects:
        if item["kind"] == "Pod":
            specs.append(item["spec"])
        elif item["kind"] == "Deployment":
            specs.append(item["spec"]["template"]["spec"])
    return specs


def test_every_object_is_owned_and_every_image_pinned() -> None:
    from curie_protected_hooks.commissioning_local_installation import OWNER_LABEL

    _, objects = rendered()
    assert all(item["metadata"]["labels"][OWNER_LABEL] == OWNER for item in objects)
    assert all(item["metadata"]["namespace"] == NAMESPACE for item in objects)
    images = {c["image"] for spec in _pod_specs(objects) for c in spec["containers"]}
    api = module()
    assert images == {api.POSTGRES_IMAGE, api.VALKEY_IMAGE, api.RUSTFS_IMAGE, API_IMAGE}
    assert all("@sha256:" in image for image in images)


def test_pods_meet_the_restricted_pod_security_standard() -> None:
    _, objects = rendered()
    for spec in _pod_specs(objects):
        assert spec["automountServiceAccountToken"] is False
        assert spec["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
        for container in spec["containers"]:
            context = container["securityContext"]
            assert context["runAsNonRoot"] is True and context["runAsUser"] > 0
            assert context["allowPrivilegeEscalation"] is False
            assert context["readOnlyRootFilesystem"] is True
            assert context["capabilities"] == {"drop": ["ALL"]}


def test_secrets_reach_pods_only_by_reference() -> None:
    secrets, objects = rendered()
    values = [getattr(secrets, name) for name in secrets.__dataclass_fields__]
    assert len(set(values)) == len(values) and all(len(v) >= 40 for v in values)
    pods = json.dumps(_pod_specs(objects))
    assert all(value not in pods for value in values)
    assert all(value not in repr(secrets) for value in values)
    (stored,) = [
        o for o in objects if o["kind"] == "Secret" and o["metadata"]["name"] == "curie-services"
    ]
    decoded = {k: base64.b64decode(v).decode() for k, v in stored["data"].items()}
    assert sorted(decoded.values()) == sorted(values)


def test_api_runs_the_production_composition_with_the_runtime_directory() -> None:
    api = module()
    _, objects = rendered()
    (deployment,) = [o for o in objects if o["kind"] == "Deployment"]
    (container,) = deployment["spec"]["template"]["spec"]["containers"]
    assert "command" not in container and "args" not in container
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["CURIE_PROTECTED_RUNTIME_DIR"] == api.RUNTIME_DIR
    assert env["ENVIRONMENT"] == "prod"
    mounts = {m["mountPath"]: m for m in container["volumeMounts"]}
    assert mounts[api.RUNTIME_DIR]["readOnly"] is True
    volumes = {v["name"]: v for v in deployment["spec"]["template"]["spec"]["volumes"]}
    assert volumes["runtime"]["secret"]["secretName"] == "curie-api-protected-runtime"
    assert container["readinessProbe"]["httpGet"]["path"] == "/ready"
    migrate = next(o for o in objects if o["metadata"]["name"] == "curie-migrate")
    assert migrate["spec"]["restartPolicy"] == "Never"
    assert migrate["spec"]["containers"][0]["command"] == [
        "python",
        "-m",
        "curie_api.schema_compat",
        "upgrade",
    ]


def test_runtime_secret_carries_the_rendered_files_unchanged() -> None:
    files = {"manifest.json": b'{"a":1}', "ca.pem": b"-----BEGIN-----"}
    secret = module().runtime_secret(OWNER, NAMESPACE, files)
    assert secret["immutable"] is True
    assert {k: base64.b64decode(v) for k, v in secret["data"].items()} == files


@pytest.mark.parametrize("image", ["curie-api:latest", "localhost:5000/api@sha256:" + "A" * 64, ""])
def test_unpinned_api_images_refuse(image: str) -> None:
    api = module()
    with pytest.raises(Exception, match="refused"):
        api.api_objects(OWNER, NAMESPACE, image)
    with pytest.raises(Exception, match="refused"):
        api.migration_pod(OWNER, NAMESPACE, image)
