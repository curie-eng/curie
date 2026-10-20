"""Import released and caller-supplied API images into an owned kind node."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tarfile
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "cli/scripts/cluster-upgrade-matrix.sh"
RELEASED = "ghcr.io/curie-eng/curie-api:0.8.8"
NODE_IMAGE = (
    "kindest/node:v1.31.0@sha256:25a3504b2b340954595fa7a6ed1575ef2edadf5abd83c0776a4308b64bf47c93"
)


def _containerd_ref(ref: str) -> str:
    for prefix in ("index.docker.io/", "registry-1.docker.io/"):
        if ref.startswith(prefix):
            ref = "docker.io/" + ref[len(prefix) :]
    first = ref.split("/", 1)[0]
    if "/" not in ref:
        ref = f"docker.io/library/{ref}"
    elif "." not in first and ":" not in first and first != "localhost":
        ref = f"docker.io/{ref}"
    elif first == "docker.io" and ref.count("/") == 1:
        ref = ref.replace("docker.io/", "docker.io/library/", 1)
    if ":" not in ref.rsplit("/", 1)[-1]:
        ref += ":latest"
    return ref


def _run(
    *args: str,
    check: bool = True,
    env: dict[str, str] | None = None,
    stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=REPO,
        env=env,
        input=stdin,
        text=True,
        capture_output=True,
        timeout=600,
        check=False,
    )
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed ({result.returncode}):\n{result.stderr}")
    return result


class KindImageImport:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.name = f"curie-image-import-{uuid.uuid4().hex[:12]}"
        self.namespace = self.name
        self.kubeconfig = root / "kubeconfig"
        self.archives = root / "archives"
        self.archives.mkdir()
        self.started = False
        self.namespace_attempted = False
        self.node = f"{self.name}-control-plane"
        self.candidate = ""
        self.sha = ""
        self.platform = ""
        self.configs: dict[str, str] = {}

    def kubectl(self, *args: str, stdin: str | None = None) -> str:
        return _run(
            "kubectl",
            "--kubeconfig",
            str(self.kubeconfig),
            "--context",
            f"kind-{self.name}",
            *args,
            stdin=stdin,
        ).stdout

    def owned_nodes(self) -> list[str]:
        return _run(
            "docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={self.name}"
        ).stdout.split()

    def start(self) -> None:
        self.candidate = os.environ["CURIE_IMAGE_IMPORT_CANDIDATE"]
        self.sha = os.environ["CURIE_IMAGE_IMPORT_CANDIDATE_SHA"]
        if not self.candidate or not re.fullmatch(r"[0-9a-f]{40}", self.sha):
            raise RuntimeError("Require a candidate image and its full checkout SHA")
        if _run("git", "rev-parse", "HEAD").stdout.strip() != self.sha:
            raise RuntimeError("Candidate SHA must equal the current checkout HEAD")
        revision = _run(
            "docker",
            "image",
            "inspect",
            "--format",
            '{{index .Config.Labels "org.opencontainers.image.revision"}}',
            self.candidate,
        ).stdout.strip()
        if revision != self.sha:
            raise RuntimeError("Candidate image revision label must equal the supplied SHA")
        if _run("kind", "version").stdout.split()[:2] != ["kind", "v0.24.0"]:
            raise RuntimeError("This smoke requires kind v0.24.0")
        self.kubectl("version", "--client=true")
        self.platform = _run(
            "docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"
        ).stdout.strip()
        _run("docker", "pull", f"--platform={self.platform}", RELEASED)
        raw = self.root / "expected-images.tar"
        try:
            _run("docker", "image", "save", "--output", str(raw), RELEASED, self.candidate)
            native: dict[str, str] = {}
            with tarfile.open(raw) as archive:
                manifest_file = archive.extractfile("manifest.json")
                if manifest_file is None:
                    raise RuntimeError("Plain docker save has no readable Docker manifest")
                with manifest_file:
                    manifest = json.load(manifest_file)
                for entry in manifest:
                    config_file = archive.extractfile(entry["Config"])
                    if config_file is None:
                        raise RuntimeError("Plain docker save has no readable image config")
                    with config_file:
                        config_bytes = config_file.read()
                    config = json.loads(config_bytes)
                    if f"{config['os']}/{config['architecture']}" != self.platform:
                        continue
                    digest = "sha256:" + hashlib.sha256(config_bytes).hexdigest()
                    for tag in entry["RepoTags"]:
                        normalized = _containerd_ref(tag)
                        if normalized in native and native[normalized] != digest:
                            raise RuntimeError("Plain docker save has ambiguous native configs")
                        native[normalized] = digest
            self.configs = {ref: native[_containerd_ref(ref)] for ref in (RELEASED, self.candidate)}
        finally:
            raw.unlink(missing_ok=True)
        clusters = _run("kind", "get", "clusters").stdout.split()
        if {self.name, f"{self.name}-missing"} & set(clusters) or self.owned_nodes():
            raise RuntimeError("Refusing an existing kind cluster or node")
        if not _run("docker", "image", "inspect", self.missing_image, check=False).returncode:
            raise RuntimeError("Negative-control image unexpectedly exists")
        self.started = True
        _run(
            "kind",
            "create",
            "cluster",
            "--name",
            self.name,
            "--image",
            NODE_IMAGE,
            "--kubeconfig",
            str(self.kubeconfig),
            "--wait",
            "180s",
        )
        if len(self.owned_nodes()) != 1:
            raise RuntimeError("Expected exactly one owned kind node")
        self.namespace_attempted = True
        self.kubectl("create", "namespace", self.namespace)

    @property
    def missing_image(self) -> str:
        return f"curie-image-import-missing:{self.name}"

    def load(self, *refs: str, missing_cluster: bool = False) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "CURIE_E2E_KIND_CLUSTER": self.name + ("-missing" if missing_cluster else ""),
                "CURIE_E2E_KUBECONFIG": str(self.kubeconfig),
                "TMPDIR": str(self.archives),
                "DOCKER_API_VERSION": "1.45",
            }
        )
        return _run(
            "bash",
            "-c",
            'source "$1"; load_kind_images "${@:2}" || exit $?',
            "kind-image-import",
            str(SCRIPT),
            *refs,
            env=env,
            check=False,
        )

    def content(self, digest: str) -> dict:
        return json.loads(
            _run(
                "docker", "exec", self.node, "ctr", "-n", "k8s.io", "content", "get", digest
            ).stdout
        )

    def close(self) -> None:
        if not self.started:
            return
        try:
            if self.namespace_attempted:
                self.kubectl(
                    "delete",
                    "namespace",
                    self.namespace,
                    "--ignore-not-found=true",
                    "--wait=true",
                    "--timeout=90s",
                )
                remaining = self.kubectl(
                    "get", "namespace", self.namespace, "--ignore-not-found=true", "-o", "name"
                )
                if remaining.strip():
                    raise RuntimeError("Owned namespace remains after cleanup")
        finally:
            try:
                _run(
                    "kind",
                    "delete",
                    "cluster",
                    "--name",
                    self.name,
                    "--kubeconfig",
                    str(self.kubeconfig),
                )
            finally:
                if self.owned_nodes():
                    raise RuntimeError("Owned kind node remains after cleanup")
                if any(self.archives.iterdir()):
                    raise RuntimeError("Image loader left temporary archives")


@pytest.fixture
def kind_image_import(tmp_path: Path, request: pytest.FixtureRequest) -> KindImageImport:
    probe = KindImageImport(tmp_path)
    request.addfinalizer(probe.close)
    probe.start()
    return probe


def test_kind_image_import_released_and_candidate(kind_image_import: KindImageImport) -> None:
    probe = kind_image_import
    # Observed Docker 29: plain 0.8.8 save retained absent arm64 OCI descriptors;
    # kind v0.24 failed on that graph, then imported its native Docker v2 members.
    # inspect .Id was the OCI index digest, so setup hashes actual config bytes
    # from an independent plain save. Filenames need not encode the config SHA.
    # containerd prefers oci-layout over manifest.json when both are present:
    # https://github.com/containerd/containerd/blob/v1.7.18/images/archive/importer.go
    # --platform on save requires API 1.48; the loader runs at baseline API 1.45:
    # https://docs.docker.com/reference/cli/docker/image/save/#platform
    # https://github.com/kubernetes-sigs/kind/blob/v0.24.0/pkg/cluster/nodeutils/util.go
    for refs in ((RELEASED,), (RELEASED, probe.candidate)):
        imported = probe.load(*refs)
        assert imported.returncode == 0, imported.stderr
        assert list(probe.archives.iterdir()) == []

    tags = _run(
        "docker", "exec", probe.node, "ctr", "-n", "k8s.io", "images", "list"
    ).stdout.splitlines()
    os_name, architecture = probe.platform.split("/")
    for index, ref in enumerate((RELEASED, probe.candidate)):
        normalized = _containerd_ref(ref)
        matches = [row.split()[2] for row in tags[1:] if row.split()[0] == normalized]
        assert len(matches) == 1, f"Missing containerd tag: {normalized}"
        manifest = probe.content(matches[0])
        if "manifests" in manifest:
            native = [
                entry
                for entry in manifest["manifests"]
                if entry.get("platform", {}).get("os") == os_name
                and entry.get("platform", {}).get("architecture") == architecture
            ]
            assert len(native) == 1, manifest
            manifest = probe.content(native[0]["digest"])
        assert manifest["config"]["digest"] == probe.configs[ref]
        config = probe.content(manifest["config"]["digest"])
        if ref == probe.candidate:
            assert config["config"]["Labels"]["org.opencontainers.image.revision"] == probe.sha
        pod = f"api-import-{index}"
        probe.kubectl(
            "-n",
            probe.namespace,
            "apply",
            "-f",
            "-",
            stdin=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": pod, "namespace": probe.namespace},
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "api",
                                "image": ref,
                                "imagePullPolicy": "Never",
                                "command": [
                                    "python3",
                                    "-c",
                                    "import curie_api; print('api-import-ok')",
                                ],
                            }
                        ],
                    },
                }
            ),
        )
        probe.kubectl(
            "-n",
            probe.namespace,
            "wait",
            "--for=jsonpath={.status.phase}=Succeeded",
            f"pod/{pod}",
            "--timeout=180s",
        )
        assert probe.kubectl("-n", probe.namespace, "logs", pod).strip() == "api-import-ok"

    for refs, missing_cluster in (((probe.missing_image,), False), ((RELEASED,), True)):
        failed = probe.load(*refs, missing_cluster=missing_cluster)
        assert failed.returncode != 0, "Negative-control import unexpectedly succeeded"
        assert list(probe.archives.iterdir()) == []
