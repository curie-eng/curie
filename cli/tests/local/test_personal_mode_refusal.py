"""Personal subscription mode is refused by the Docker-backed CLI before any Docker call.

Drives the built candidate binary against the Docker engine the environment points at
(DOCKER_HOST or the active context). Refusals must leave no container, volume or
network behind; the same `skill up --fake-model` without the variable is the service
control and must start its runner, then be torn down.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
PERSONAL_VARS = ("CURIE_MODEL_AUTH_MODE", "FACTORY_MODEL_AUTH_MODE")
CODE = "personal_mode_requires_kubernetes"
MANAGED_LABEL = "label=curietech.ai/managed-by=curie-cli"
RUNNER_IMAGE = "curie-runner"


def _docker(*args: str) -> str:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=60, check=False
    )
    if result.returncode:
        raise RuntimeError(f"docker {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class Trial:
    binary: Path
    bundle: Path
    runner: str
    project: str
    port: int
    baseline_volumes: set[str]

    def env(self, **extra: str) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if k not in PERSONAL_VARS}
        env.update(extra)
        return env

    def run(
        self, *args: str, cwd: Path | None = None, **env: str
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.binary), *args],
            cwd=cwd or self.bundle,
            env=self.env(**env),
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )

    def containers(self) -> list[str]:
        by_name = _docker("ps", "-aq", "--no-trunc", "--filter", f"name=^{self.runner}$").split()
        by_project = _docker(
            "ps",
            "-aq",
            "--no-trunc",
            "--filter",
            f"label=com.docker.compose.project={self.project}",
        ).split()
        return by_name + by_project

    def networks(self) -> list[str]:
        return _docker(
            "network", "ls", "-q", "--filter", f"label=com.docker.compose.project={self.project}"
        ).split()

    def new_volumes(self) -> set[str]:
        managed = set(_docker("volume", "ls", "-q", "--filter", MANAGED_LABEL).split())
        project = set(
            _docker(
                "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={self.project}"
            ).split()
        )
        return (managed - self.baseline_volumes) | project

    def leftovers(self) -> dict[str, object]:
        return {
            "containers": self.containers(),
            "networks": self.networks(),
            "volumes": sorted(self.new_volumes()),
        }

    def close(self) -> None:
        errors: list[str] = []
        try:
            for container in self.containers():
                _docker("rm", "-f", container)
            for volume in self.new_volumes():
                _docker("volume", "rm", "-f", volume)
            for network in self.networks():
                _docker("network", "rm", network)
            remaining = self.leftovers()
            if any(remaining.values()):
                errors.append(f"Trial resources remain after cleanup: {remaining}")
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            errors.append(f"Trial cleanup failed: {exc}")
        if errors:
            raise RuntimeError("\n".join(errors))


@pytest.fixture
def trial(tmp_path: Path, request: pytest.FixtureRequest) -> Trial:
    binary = Path(os.environ.get("CURIE_BIN", ROOT / "cli/target/debug/curie")).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(
            "A built candidate CLI is required through CURIE_BIN or cli/target/debug/curie"
        )
    if shutil.which("docker") is None:
        raise RuntimeError("The actual Docker CLI is required")
    _docker("info", "--format", "{{.ServerVersion}}")
    suffix = uuid.uuid4().hex[:10]
    bundle = tmp_path / "acme-refusal"
    scaffold = subprocess.run(
        [str(binary), "init", "acme-refusal", "--dir", str(bundle)],
        env={k: v for k, v in os.environ.items() if k not in PERSONAL_VARS},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if scaffold.returncode:
        raise RuntimeError(f"curie init failed: {scaffold.stdout}{scaffold.stderr}")
    instance = Trial(
        binary=binary,
        bundle=bundle,
        runner=f"curie-refusal-{suffix}",
        project=f"curie-refusal-{suffix}",
        port=_free_port(),
        baseline_volumes=set(_docker("volume", "ls", "-q", "--filter", MANAGED_LABEL).split()),
    )
    request.addfinalizer(instance.close)
    return instance


@pytest.fixture
def runner_image(trial: Trial) -> str:
    if not _docker("image", "ls", "-q", RUNNER_IMAGE).strip():
        raise RuntimeError(f"The {RUNNER_IMAGE} image is absent on the engine; run `curie build`")
    return RUNNER_IMAGE


def _commands(trial: Trial) -> dict[str, list[str]]:
    return {
        "skill-up": [
            "skill",
            "up",
            "--fake-model",
            "--name",
            trial.runner,
            "--port",
            str(trial.port),
        ],
        "skill-eval": ["skill", "eval", "--model", "acme-model", "--json"],
        "local-up": ["local", "up", "--project", trial.project],
        "local-up-dry-run": ["local", "up", "--project", trial.project, "--dry-run"],
    }


@pytest.mark.parametrize("variable", PERSONAL_VARS)
@pytest.mark.parametrize("command", ["skill-up", "skill-eval", "local-up", "local-up-dry-run"])
def test_personal_mode_is_refused_before_any_docker_resource_exists(
    trial: Trial, command: str, variable: str
) -> None:
    argv = (
        [*_commands(trial)[command], "--json"]
        if command != "skill-eval"
        else _commands(trial)[command]
    )
    result = trial.run(*argv, **{variable: "personal"})
    payload = json.loads(result.stdout)
    assert result.returncode == 2, result.stdout + result.stderr
    assert payload["error"].startswith(CODE)
    assert variable in payload["error"]
    assert "Kubernetes" in payload["error"]
    assert variable in payload["fix"]
    assert trial.leftovers() == {"containers": [], "networks": [], "volumes": []}


def test_a_non_personal_value_still_starts_the_runner_and_down_removes_it(
    trial: Trial, runner_image: str
) -> None:
    result = trial.run(
        *_commands(trial)["skill-up"],
        "--json",
        CURIE_MODEL_AUTH_MODE="service",
        FACTORY_MODEL_AUTH_MODE="",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert CODE not in result.stdout + result.stderr
    assert len(trial.containers()) == 1
    down = trial.run("skill", "down", "--name", trial.runner, "--json")
    assert down.returncode == 0, down.stdout + down.stderr
    assert trial.containers() == []
