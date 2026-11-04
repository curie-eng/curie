"""Full-profile otel-collector-perms must not leak an anonymous volume (#5362).

The one-shot reuses the RustFS image, which declares VOLUME /data. This service
does not use that path, so Docker attaches an unlabeled anonymous volume.
``docker compose down -v`` removes declared project volumes, and it removes an
anonymous volume only while that volume's container still exists. Replacing
/data with a named project volume keeps the wipe inside the project sweep.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
COMPOSE_FILE = "compose.dev.yaml"
PROFILES = ("core", "full", "local-model", "slack")
SERVICE = "otel-collector-perms"


def _run(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"{purpose} failed with {result.returncode}: {detail}")
    return result.stdout


def _compose(project: str, *args: str) -> subprocess.CompletedProcess[str]:
    command = ["docker", "compose"]
    for profile in PROFILES:
        command.extend(["--profile", profile])
    command.extend(["-p", project, "-f", COMPOSE_FILE, *args])
    return _run(command)


def _volume_ids() -> set[str]:
    listed = _require(_run(["docker", "volume", "ls", "-q"]), "listing volumes")
    return {line.strip() for line in listed.splitlines() if line.strip()}


def _project_volume_ids(project: str) -> set[str]:
    listed = _require(
        _run(
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ]
        ),
        "listing project volumes",
    )
    return {line.strip() for line in listed.splitlines() if line.strip()}


@dataclass
class Cycle:
    project: str
    foreign: str
    project_volumes_before: set[str]
    mounts: list[dict[str, object]] = field(default_factory=list)
    mounted: list[str] = field(default_factory=list)

    def volume_labels(self, name: str) -> dict[str, str]:
        raw = _require(
            _run(["docker", "volume", "inspect", name, "--format", "{{json .Labels}}"]),
            "inspecting a volume",
        ).strip()
        parsed = json.loads(raw)
        return parsed or {}

    def down(self) -> None:
        _require(_compose(self.project, "down", "-v"), "wiping the project")


def _cleanup(cycle: Cycle) -> None:
    errors: list[str] = []
    down = _compose(cycle.project, "down", "-v")
    if down.returncode != 0:
        errors.append((down.stderr or down.stdout or "down failed").strip())
    containers = _run(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label=com.docker.compose.project={cycle.project}",
        ]
    )
    if containers.returncode == 0 and containers.stdout.strip():
        removed = _run(["docker", "rm", "-f", "-v", *containers.stdout.split()])
        if removed.returncode != 0:
            errors.append((removed.stderr or "container cleanup failed").strip())
    names = _project_volume_ids(cycle.project) | set(cycle.mounted) | {cycle.foreign}
    for name in sorted(names):
        if name not in _volume_ids():
            continue
        removed = _run(["docker", "volume", "rm", name])
        if removed.returncode != 0 and "No such volume" not in removed.stderr:
            errors.append((removed.stderr or f"could not remove {name}").strip())
    networks = _run(
        [
            "docker",
            "network",
            "ls",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={cycle.project}",
        ]
    )
    if networks.returncode == 0 and networks.stdout.strip():
        removed = _run(["docker", "network", "rm", *networks.stdout.split()])
        if removed.returncode != 0:
            errors.append((removed.stderr or "network cleanup failed").strip())
    if _project_volume_ids(cycle.project):
        errors.append("project volumes remained after cleanup")
    still = _run(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label=com.docker.compose.project={cycle.project}",
        ]
    )
    if still.returncode != 0 or still.stdout.strip():
        errors.append("project containers remained after cleanup")
    if cycle.foreign in _volume_ids():
        errors.append("the unrelated volume remained after cleanup")
    for name in cycle.mounted:
        if name in _volume_ids():
            errors.append(f"mounted volume {name} remained after cleanup")
    if errors:
        raise RuntimeError("; ".join(errors))


@pytest.fixture
def cycle() -> Iterator[Cycle]:
    _require(_run(["docker", "info", "--format", "{{.ServerVersion}}"]), "reaching Docker")
    project = f"c5362{uuid.uuid4().hex[:10]}"
    foreign = f"c5362foreign{uuid.uuid4().hex[:10]}"
    started = Cycle(
        project=project,
        foreign=foreign,
        project_volumes_before=_project_volume_ids(project),
    )
    try:
        _require(_run(["docker", "volume", "create", foreign]), "creating an unrelated volume")
        _require(
            _compose(project, "up", "-d", "--no-deps", SERVICE),
            "starting otel-collector-perms",
        )
        listed = _require(
            _run(
                [
                    "docker",
                    "ps",
                    "-aq",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            ),
            "listing project containers",
        )
        mounts: list[dict[str, object]] = []
        for container_id in listed.split():
            raw = _require(
                _run(["docker", "inspect", container_id, "--format", "{{json .Mounts}}"]),
                "inspecting container mounts",
            )
            mounts.extend(json.loads(raw))
        started.mounts = [mount for mount in mounts if mount.get("Type") == "volume"]
        started.mounted = [str(mount["Name"]) for mount in started.mounts]
        yield started
    finally:
        _cleanup(started)


def test_full_profile_perms_wipe_leaves_no_anonymous_volume(cycle: Cycle) -> None:
    """A full-profile up and wipe leaves no volume this service mounted.

    An unrelated volume created beside the project must still exist after the
    wipe. Cleanup of that volume happens after the assertion.
    """

    # Observed on the engine: rustfs/rustfs:1.0.0-beta.12 declares VOLUME /data.
    # An uncovered image volume is anonymous and labeled only
    # com.docker.volume.anonymous. compose down -v deletes that volume only
    # while its container still exists, so the wipe below runs after the
    # one-shot container has been removed without its volumes.
    data = [mount for mount in cycle.mounts if mount.get("Destination") == "/data"]
    assert len(data) == 1
    data_labels = cycle.volume_labels(str(data[0]["Name"]))
    assert data_labels.get("com.docker.compose.project") == cycle.project
    assert data_labels.get("com.docker.compose.volume") == "otel_collector_perms_data"
    assert "com.docker.volume.anonymous" not in data_labels
    for mount in cycle.mounts:
        labels = cycle.volume_labels(str(mount["Name"]))
        assert labels.get("com.docker.compose.project") == cycle.project
    assert cycle.project_volumes_before == set()
    containers = _require(
        _run(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                f"label=com.docker.compose.project={cycle.project}",
            ]
        ),
        "listing the one-shot container",
    ).split()
    assert containers
    _require(_run(["docker", "rm", "-f", *containers]), "removing the one-shot container")
    cycle.down()
    assert _project_volume_ids(cycle.project) == set()
    assert cycle.foreign in _volume_ids()
    for name in cycle.mounted:
        assert name not in _volume_ids()
