"""Production protected worker of a local installation, @spec PROTECTED-COMMISSIONING-6.

The rendered runtime files are written as regular files and read back with the
worker's own loaders; the cluster test installs the same objects.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.util
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509

from .commissioning_worker_vectors import worker_profile
from .test_commissioning_local_broker import principals
from .test_commissioning_provisioner import profile_fields

OWNER = "curie-commission-0123456789ab"
NAMESPACE = OWNER + "-protected"
WORKER_IMAGE = "localhost:5000/" + OWNER + "/worker@sha256:" + "b" * 64


def module() -> Any:
    name = "curie_protected_hooks.commissioning_local_worker"
    assert importlib.util.find_spec(name) is not None, "local worker not implemented"
    return importlib.import_module(name)


def rendered() -> tuple[Any, Any, dict[str, bytes], Any]:
    from curie_protected_hooks.commissioning_acl import GuardRecipes
    from curie_protected_hooks.commissioning_local_bootstrap import (
        LaunchMaterial,
        LaunchPublic,
        issue_credentials,
        mediator_name,
    )
    from curie_protected_hooks.commissioning_local_workloads import ServiceSecrets
    from curie_protected_hooks.commissioning_provisioner import construct

    api = module()
    chosen = worker_profile()
    result = construct(chosen, acl=GuardRecipes(profile_fields()["qualification_id"]))
    worker = issue_credentials(principals())["worker"]
    material = LaunchMaterial.generate(mediator_name(NAMESPACE))
    launch = LaunchPublic(
        material.server_name,
        material.ca_pem,
        material.server_spki_sha256,
        material.client_spki_sha256,
    )
    secrets = ServiceSecrets.fresh()
    files = api.worker_files(
        manifest=result.manifest,
        execution_config=chosen.execution_config,
        projection=chosen.projection,
        measurement_file=result.measurement_file,
        ca_pem="-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n",
        worker=worker,
        launch=launch,
        namespace=NAMESPACE,
        secrets=secrets,
    )
    return result, material, files, (worker, secrets)


def write(root: Path, files: dict[str, bytes]) -> str:
    for path, raw in files.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    return str(root)


def test_the_worker_loaders_accept_the_rendered_files(tmp_path: Path) -> None:
    from curie_worker.protected_entry import load_process_settings
    from curie_worker.protected_worker_bootstrap import load_worker_bootstrap

    result, launch, files, (worker, secrets) = rendered()
    directory = write(tmp_path / "runtime", files)
    bootstrap = load_worker_bootstrap(directory)
    assert bootstrap.manifest.canonical_bytes == result.manifest.canonical_bytes
    assert bootstrap.credential.username == worker.username
    settings = load_process_settings(directory)
    assert settings.internal_worker_token == secrets.internal_worker_token
    assert settings.db_schema == "curie"
    tls = settings.launch_tls
    assert tls.server_spki_sha256 == launch.server_spki_sha256
    from curie_protected_hooks.commissioning_local_bootstrap import mediator_name

    assert tls.server_name == launch.server_name == mediator_name(NAMESPACE)
    assert tls.worker_principal_id == "worker/" + result.manifest.as_dict()["runtime_id"]
    assert tls.ca_file == module().RUNTIME_DIR + "/launch-ca.pem"


def test_content_addressed_files_carry_their_digests() -> None:
    result, _, files, _ = rendered()
    name, raw = result.measurement_file
    assert files["measurements/" + name] == raw
    projection = worker_profile().projection
    assert files["projections/" + hashlib.sha256(projection).hexdigest() + ".json"] == projection


def test_the_worker_binds_the_projection_to_the_measurement(tmp_path: Path) -> None:
    """What the worker checks once a Qualification exists, checked here in advance."""
    from curie_worker.protected_kubernetes_projection import ProjectionDirectory
    from curie_worker.protected_worker_bootstrap import load_worker_bootstrap

    result, _, files, _ = rendered()
    directory = write(tmp_path / "runtime", files)
    bootstrap = load_worker_bootstrap(directory)
    with ProjectionDirectory(directory + "/projections") as projections:
        projection = projections.load(
            manifest=result.manifest, measurement=result.measurement, execution=bootstrap.execution
        )
    assert projection is not None


def test_the_launch_identity_pins_the_mediator_and_signs_the_worker_client() -> None:
    _, launch, files, _ = rendered()
    ca = x509.load_pem_x509_certificate(launch.ca_pem.encode())
    server = x509.load_pem_x509_certificate(launch.server_certificate_pem.encode())
    client = x509.load_pem_x509_certificate(launch.client_certificate_pem.encode())
    server.verify_directly_issued_by(ca)
    client.verify_directly_issued_by(ca)
    san = server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert san.get_values_for_type(x509.DNSName) == [launch.server_name]
    assert launch.server_spki_sha256 != launch.client_spki_sha256
    # The provisioner renders no private key: both stay in the administrator's Secrets.
    rendered_bytes = b"".join(files.values())
    assert b"PRIVATE KEY" not in rendered_bytes
    assert "launch-client.key" not in files and "launch-client.pem" not in files
    assert launch.server_key_pem not in repr(launch)


def test_objects_stage_regular_files_and_run_the_production_entry() -> None:
    from curie_protected_hooks.commissioning_local_installation import OWNER_LABEL

    api = module()
    _, _, files, _ = rendered()
    runtime, deployment = api.worker_objects(OWNER, NAMESPACE, WORKER_IMAGE, files)
    assert all(o["metadata"]["labels"][OWNER_LABEL] == OWNER for o in (runtime, deployment))
    assert runtime["immutable"] is True
    decoded = {k: base64.b64decode(v) for k, v in runtime["data"].items()}
    assert decoded == {path.replace("/", "."): raw for path, raw in files.items()}
    spec = deployment["spec"]["template"]["spec"]
    (stage,) = spec["initContainers"]
    (worker,) = spec["containers"]
    assert worker["command"] == [
        "python", "-m", "curie_worker.protected_entry", "--runtime-dir", api.RUNTIME_DIR,
    ]  # fmt: skip
    assert stage["image"] == worker["image"] == WORKER_IMAGE
    runtime_mounts = [m for m in worker["volumeMounts"] if m["mountPath"] == api.RUNTIME_DIR]
    assert runtime_mounts == [{"name": "runtime", "mountPath": api.RUNTIME_DIR, "readOnly": True}]
    volumes = {v["name"]: v for v in spec["volumes"]}
    assert "emptyDir" in volumes["runtime"]
    from curie_protected_hooks.commissioning_local_bootstrap import WORKER_CLIENT_SECRET

    runtime_source, client_source = volumes["source"]["projected"]["sources"]
    assert {i["path"] for i in runtime_source["secret"]["items"]} == set(files)
    assert client_source["secret"]["name"] == WORKER_CLIENT_SECRET
    assert {i["path"] for i in client_source["secret"]["items"]} == {
        "launch-client.pem",
        "launch-client.key",
    }
    for container in (stage, worker):
        assert container["securityContext"]["runAsNonRoot"] is True
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
    pod = json.dumps(spec)
    _, _, _, (credential, secrets) = rendered()
    assert credential.password not in pod and secrets.internal_worker_token not in pod


@pytest.mark.parametrize("image", ["curie-worker:latest", ""])
def test_unpinned_worker_images_refuse(image: str) -> None:
    api = module()
    _, _, files, _ = rendered()
    with pytest.raises(Exception, match="refused"):
        api.worker_objects(OWNER, NAMESPACE, image, files)


def projected_volume(root: Path, files: dict[str, bytes]) -> None:
    """The kubelet's atomic-writer layout: a timestamped directory, ``..data``, symlinks."""
    stamp = root / "..2026_10_21_00_00_00.000000000"
    for path, raw in files.items():
        target = stamp / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    (root / "..data").symlink_to(stamp.name)
    for top in {path.split("/", 1)[0] for path in files}:
        (root / top).symlink_to("..data/" + top)


def test_the_stage_copies_exactly_the_runtime_files(tmp_path: Path) -> None:
    api = module()
    _, _, files, _ = rendered()
    client = {"launch-client.pem": b"certificate", "launch-client.key": b"key"}
    source, runtime = tmp_path / "source", tmp_path / "runtime"
    source.mkdir()
    runtime.mkdir()
    projected_volume(source, {**files, **client})
    _, deployment = api.worker_objects(OWNER, NAMESPACE, WORKER_IMAGE, files)
    (stage,) = deployment["spec"]["template"]["spec"]["initContainers"]
    *shell, script = stage["command"]
    script = script.replace(api.SOURCE_DIR, str(source)).replace(api.RUNTIME_DIR, str(runtime))
    subprocess.run([*shell, script], check=True)
    copied = {
        str(Path(d, f).relative_to(runtime)): Path(d, f)
        for d, _, names in os.walk(runtime)
        for f in names
    }
    assert set(copied) == set(files) | set(client)
    assert not any(p.is_symlink() for p in runtime.rglob("*"))
    assert all(copied[p].read_bytes() == raw for p, raw in {**files, **client}.items())


@pytest.mark.parametrize(
    "path",
    [
        "extra.json",
        "..data",
        "measurements/latest.json",
        "projections/a/b.json",
        "launch-client.key",
    ],
)
def test_runtime_files_outside_the_grammar_refuse(path: str) -> None:
    api = module()
    _, _, files, _ = rendered()
    with pytest.raises(Exception, match="refused"):
        api.worker_objects(OWNER, NAMESPACE, WORKER_IMAGE, {**files, path: b"{}"})
