"""The opt in Alertmanager signer has a reachable and secret backed install."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY = ROOT / "examples" / "sre-bot" / "observability"
SIGNER = OBSERVABILITY / "alert-signer.yaml"


def _resources() -> dict[str, dict]:
    assert SIGNER.is_file()
    documents = [doc for doc in yaml.safe_load_all(SIGNER.read_text()) if doc]
    assert len(documents) == len({doc["kind"] for doc in documents})
    return {doc["kind"]: doc for doc in documents}


def test_signer_service_selects_the_deployment_and_exposes_port_8080() -> None:
    resources = _resources()
    deployment = resources["Deployment"]
    service = resources["Service"]
    assert service["metadata"]["name"] == "alert-signer"
    selector = service["spec"]["selector"]
    pod_labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert selector and all(pod_labels.get(key) == value for key, value in selector.items())
    container, = deployment["spec"]["template"]["spec"]["containers"]
    ports = container.get("ports") or []
    assert any(port.get("containerPort") == 8080 for port in ports)
    assert any(
        port.get("port") == 8080
        and port.get("targetPort") in (8080, *(entry.get("name") for entry in ports))
        for port in service["spec"]["ports"]
    )


def test_signer_receives_hook_configuration_from_operator_secret() -> None:
    deployment = _resources()["Deployment"]
    container, = deployment["spec"]["template"]["spec"]["containers"]
    env = {entry["name"]: entry for entry in container.get("env") or []}
    assert {"CURIE_HOOK_URL", "CURIE_HOOK_SECRET", "CURIE_SIGNER_TOKEN"} <= set(env)
    for name in ("CURIE_HOOK_SECRET", "CURIE_SIGNER_TOKEN"):
        assert (env[name].get("valueFrom") or {}).get("secretKeyRef"), name
        assert "value" not in env[name], name
    assert env["CURIE_HOOK_URL"].get("value") or env["CURIE_HOOK_URL"].get("valueFrom")


def test_signer_deployment_has_a_runnable_server() -> None:
    deployment = _resources()["Deployment"]
    pod = deployment["spec"]["template"]["spec"]
    (container,) = pod["containers"]
    assert container.get("image"), "the signer needs a provisionable runtime image"
    command = [*container.get("command", []), *container.get("args", [])]
    assert "/app/server.py" in command
    mounts = [
        mount
        for mount in container.get("volumeMounts") or []
        if mount.get("mountPath") == "/app"
    ]
    assert len(mounts) == 1
    assert mounts[0].get("readOnly") is True
    volumes = {volume["name"]: volume for volume in pod.get("volumes") or []}
    assert (volumes[mounts[0]["name"]].get("configMap") or {}).get("name") == (
        "alert-signer-code"
    )


def test_default_observability_does_not_enable_the_signer() -> None:
    default = yaml.safe_load((OBSERVABILITY / "prometheus-values.yaml").read_text())
    assert (default.get("alertmanager") or {}).get("enabled") is not True
    assert "alert-signer" not in (OBSERVABILITY / "curie-values.yaml").read_text()
