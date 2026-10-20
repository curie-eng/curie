"""Import released and caller-supplied API images into an owned kind node."""

from __future__ import annotations

import json
import os
import re
import subprocess
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "cli/scripts/cluster-upgrade-matrix.sh"
RELEASED = "ghcr.io/curie-eng/curie-api:0.8.8"
NODE_IMAGE = (
    "kindest/node:v1.31.0@sha256:25a3504b2b340954595fa7a6ed1575ef2edadf5abd83c0776a4308b64bf47c93"
)


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
        for ref in (RELEASED, self.candidate):
            self.configs[ref] = _run(
                "docker", "image", "inspect", "--format", "{{.Id}}", ref
            ).stdout.strip()
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
    # Observed Docker 29 native-only 0.8.8 pull: plain save retained absent arm64
    # descriptors; kind v0.24 imports --all-platforms and failed on a missing digest.
    # --platform exported a complete archive which the same kind node imported:
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
        first = ref.split("/", 1)[0]
        normalized = ref
        if "/" not in ref:
            normalized = f"docker.io/library/{ref}"
        elif "." not in first and ":" not in first and first != "localhost":
            normalized = f"docker.io/{ref}"
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
